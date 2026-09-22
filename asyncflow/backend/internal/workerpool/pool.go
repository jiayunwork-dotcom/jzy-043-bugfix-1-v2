// Package workerpool implements in-process executors: each Worker has a fixed
// number of concurrency slots, heartbeats, renews task leases, reacts to
// preemption cancellation and supports graceful shutdown.
package workerpool

import (
	"context"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// Reporter finalizes an attempt (success or failure, retry or dead-letter).
type Reporter interface {
	ReportResult(ctx context.Context, t *domain.Task, success bool, result []byte, errMsg, category string)
}

// Handlers maps task type -> handler functions; several handlers may exist
// across workers for the same type (load balanced by the scheduler).
type Handlers map[string]domain.HandlerFunc

// inflight tracks one running task's cancelable context and lease timer.
type inflight struct {
	task      *domain.Task
	cancel    context.CancelFunc
	deadline  time.Time
	preempted bool // set by Preempt() to distinguish preemption cancellation
}

// Worker is one in-process executor.
type Worker struct {
	id           string
	name         string
	capabilities map[string]bool
	slots        int
	handlers     Handlers
	reporter     Reporter
	leaseSeconds int
	renewEvery   time.Duration
	heartbeat    func(w *Snapshot)

	mu       sync.Mutex
	tasks    map[string]*inflight // taskID -> inflight
	reserved map[string]bool      // reserved but not yet delivered
	draining bool
	closed   bool
	renewer  LeaseRenewer
	wg       sync.WaitGroup
}

// Snapshot is passed to the heartbeat sink.
type Snapshot struct {
	ID           string
	Name         string
	Capabilities []string
	TotalSlots   int
	UsedSlots    int
	CurrentTasks []string
	Draining     bool
}

// LeaseRenewer extends a task lease while it runs; returns false if lost.
type LeaseRenewer interface {
	RenewLease(ctx context.Context, taskID, workerID string, lease time.Duration) bool
}

// Options constructs a Worker.
type Options struct {
	ID           string
	Name         string
	Handlers     Handlers
	Slots        int
	Reporter     Reporter
	Renewer      LeaseRenewer
	LeaseSeconds int
	Heartbeat    func(w *Snapshot)
}

func New(o Options) *Worker {
	caps := map[string]bool{}
	for t := range o.Handlers {
		caps[t] = true
	}
	lease := o.LeaseSeconds
	if lease <= 0 {
		lease = 30
	}
	return &Worker{
		id:           o.ID,
		name:         o.Name,
		capabilities: caps,
		slots:        o.Slots,
		handlers:     o.Handlers,
		reporter:     o.Reporter,
		renewer:      o.Renewer,
		leaseSeconds: lease,
		renewEvery:   time.Duration(lease) * time.Second / 3,
		heartbeat:    o.Heartbeat,
		tasks:        map[string]*inflight{},
		reserved:     map[string]bool{},
	}
}

func (w *Worker) ID() string { return w.id }

func (w *Worker) CanHandle(taskType string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.draining || w.closed {
		return false
	}
	if w.usedLocked() >= w.slots {
		return false
	}
	if taskType == "" {
		return true // wildcard availability probe
	}
	if w.capabilities["*"] {
		return true // worker accepts every task type
	}
	return w.capabilities[taskType]
}

func (w *Worker) usedLocked() int { return len(w.tasks) + len(w.reserved) }

// Capable reports type support without considering slot availability.
func (w *Worker) Capable(taskType string) bool {
	if taskType == "" || w.capabilities["*"] {
		return true
	}
	return w.capabilities[taskType]
}

// HasPreemptibleTask reports whether a Normal-or-lower task is running.
func (w *Worker) HasPreemptibleTask() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, it := range w.tasks {
		if it.task != nil && it.task.Priority.Rank() >= domain.PriorityNormal.Rank() {
			return true
		}
	}
	return false
}

// ReserveSlot implements scheduler.Executor.
func (w *Worker) ReserveSlot(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.draining || w.closed {
		return false
	}
	if w.usedLocked() >= w.slots {
		return false
	}
	if _, ok := w.tasks[taskID]; ok {
		return false
	}
	if w.reserved[taskID] {
		return false
	}
	w.reserved[taskID] = true
	return true
}

// ReleaseSlot frees a reservation or finished task.
func (w *Worker) ReleaseSlot(taskID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.reserved, taskID)
	delete(w.tasks, taskID)
}

// Deliver starts executing a claimed task in its own goroutine.
func (w *Worker) Deliver(ctx context.Context, t *domain.Task) error {
	w.mu.Lock()
	if !w.reserved[t.ID] {
		w.mu.Unlock()
		return nil // already gone
	}
	delete(w.reserved, t.ID)
	h, ok := w.handlers[t.Type]
	if !ok {
		w.mu.Unlock()
		w.reporter.ReportResult(ctx, t, false, nil, "no handler registered for type: "+t.Type, "handler_error")
		return nil
	}
	execCtx, cancel := context.WithCancel(ctx)
	deadline := time.Now().Add(time.Duration(t.TimeoutSeconds) * time.Second)
	if t.LeaseExpiresAt != nil {
		deadline = t.LeaseExpiresAt.Add(0)
	}
	w.tasks[t.ID] = &inflight{task: t, cancel: cancel, deadline: deadline}
	w.mu.Unlock()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.runOne(execCtx, t, h, cancel)
	}()
	return nil
}

func (w *Worker) runOne(ctx context.Context, t *domain.Task, h domain.HandlerFunc, cancel context.CancelFunc) {
	defer cancel()
	leaseRenew := w.startLeaseRenewer(ctx, t)
	defer leaseRenew()

	timeout := time.Duration(t.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	taskCtx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	res, err := runHandlerSafely(taskCtx, t, h)

	w.mu.Lock()
	wasPreempted := w.tasks[t.ID] != nil && w.tasks[t.ID].preempted
	w.mu.Unlock()

	// Postgres already flipped status if the task was preempted/reclaimed;
	// the reporter's CAS discards our result when ownership was lost. On
	// preemption the scheduler already recorded the interrupted attempt.
	if wasPreempted {
		w.ReleaseSlot(t.ID)
		return
	}
	if err != nil {
		w.reporter.ReportResult(context.Background(), t, false, nil, err.Error(), categorize(err, taskCtx))
	} else {
		w.reporter.ReportResult(context.Background(), t, true, res, "", "")
	}
	w.ReleaseSlot(t.ID)
}

func runHandlerSafely(ctx context.Context, t *domain.Task, h domain.HandlerFunc) (res []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &ExecError{Msg: "handler panic", Cat: "panic"}
		}
	}()
	return h(ctx, t)
}

// ExecError lets handlers attach an error category.
type ExecError struct {
	Msg string
	Cat string
}

func (e *ExecError) Error() string { return e.Msg }

func categorize(err error, ctx context.Context) string {
	if e, ok := err.(*ExecError); ok && e.Cat != "" {
		return e.Cat
	}
	if ctx.Err() == context.DeadlineExceeded {
		return "timeout"
	}
	m := err.Error()
	switch {
	case containsAny(m, "timeout", "deadline"):
		return "timeout"
	case containsAny(m, "panic"):
		return "panic"
	default:
		return "handler_error"
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if idx := indexOf(s, sub); idx >= 0 {
			return true
		}
	}
	return false
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Preempt cancels the running task's context (cooperative interruption).
func (w *Worker) Preempt(taskID string) {
	w.mu.Lock()
	it := w.tasks[taskID]
	if it != nil {
		it.preempted = true
	}
	w.mu.Unlock()
	if it != nil {
		it.cancel()
	}
}

// RunningTasks implements scheduler.Executor.
func (w *Worker) RunningTasks() map[string]*domain.Task {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]*domain.Task, len(w.tasks))
	for id, it := range w.tasks {
		out[id] = it.task
	}
	return out
}

// SnapshotForHeartbeat collects current state for the heartbeat loop.
func (w *Worker) SnapshotForHeartbeat() Snapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	var caps []string
	for c := range w.capabilities {
		caps = append(caps, c)
	}
	cur := make([]string, 0, len(w.tasks))
	for id := range w.tasks {
		cur = append(cur, id)
	}
	return Snapshot{
		ID: w.id, Name: w.name, Capabilities: caps,
		TotalSlots: w.slots, UsedSlots: w.usedLocked(),
		CurrentTasks: cur, Draining: w.draining,
	}
}

// HeartbeatLoop periodically pushes liveness + slot state.
func (w *Worker) HeartbeatLoop(ctx context.Context, every time.Duration) {
	if w.heartbeat == nil {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	w.heartbeat(&Snapshot{})
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snap := w.SnapshotForHeartbeat()
			w.heartbeat(&snap)
		}
	}
}

// Drain stops accepting new work and blocks until in-flight tasks finish.
func (w *Worker) Drain(ctx context.Context) {
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
}
