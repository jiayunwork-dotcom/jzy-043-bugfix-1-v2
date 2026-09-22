// Package scheduler implements the five-level priority scheduler, weighted
// fairness (forced low-priority opportunities), critical/high preemption and
// the dispatch loop that hands ready tasks to executors.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/storage"
)

// Executor is something the scheduler can hand tasks to. The embedded worker
// pool and external workers both implement it.
type Executor interface {
	ID() string
	// CanHandle reports whether this executor accepts the task type and
	// currently has a free concurrency slot.
	CanHandle(taskType string) bool
	// Capable reports type capability regardless of current slot usage —
	// used to pick a preemption victim on a currently-full executor.
	Capable(taskType string) bool
	// HasPreemptibleTask reports whether a Normal-or-lower task is in flight
	// and therefore interruptible for an arriving critical/high task.
	HasPreemptibleTask() bool
	// Deliver hands a (DB-claimed) task to the executor. It must be
	// non-blocking; returning an error means delivery failed and the
	// scheduler requeues the task and releases the reserved slot.
	Deliver(ctx context.Context, t *domain.Task) error
	// RunningTasks returns (taskID -> task) for every in-flight task.
	RunningTasks() map[string]*domain.Task
	// Preempt asks the executor to interrupt one running task (cancel ctx).
	Preempt(taskID string)
	// ReserveSlot claims a slot for a task about to be delivered.
	ReserveSlot(taskID string) bool
	// ReleaseSlot undoes a reservation / frees a finished slot.
	ReleaseSlot(taskID string)
}

// Storer is the subset of the Postgres store the scheduler needs.
type Storer interface {
	BeginAttempt(ctx context.Context, taskID, workerID string, lease time.Time) (*domain.Task, error)
	Preempt(ctx context.Context, taskID, byWorker string) (*domain.Task, error)
	GetTask(ctx context.Context, id string) (*domain.Task, error)
}

// Queuer is the scheduling data-plane.
type Queuer interface {
	EnqueueHead(ctx context.Context, p domain.Priority, taskID string) error
	Dequeue(ctx context.Context, p domain.Priority) (string, error)
	AllDepths(ctx context.Context) (map[domain.Priority]int64, error)
}

// Scheduler selects and dispatches ready tasks.
type Scheduler struct {
	store    Storer
	q        Queuer
	execs    *executorRegistry
	leaseFor time.Duration

	// Fairness: after highWatermark consecutive pops at Normal or above, a
	// lower-priority (Low/Bulk) pop is forced when such work exists.
	highWatermark int
	highStreak    int
}

func New(store Storer, q queue.Queue, leaseFor time.Duration, highWatermark int) *Scheduler {
	return &Scheduler{
		store:         store,
		q:             q,
		execs:         newRegistry(),
		leaseFor:      leaseFor,
		highWatermark: highWatermark,
	}
}

func (s *Scheduler) AddExecutor(e Executor)   { s.execs.add(e) }
func (s *Scheduler) RemoveExecutor(id string) { s.execs.remove(id) }
func (s *Scheduler) Executors() []Executor    { return s.execs.snapshot() }
func (s *Scheduler) HighStreak() int          { return s.highStreak }
func (s *Scheduler) ResetStreak()             { s.highStreak = 0 }

// availableFor reports whether a priority has queued work that the cluster
// can run right now: a free slot, or for critical/high a preemptible task.
func (s *Scheduler) availableFor(p domain.Priority, depths map[domain.Priority]int64) bool {
	if depths[p] == 0 {
		return false
	}
	preemptible := p == domain.PriorityCritical || p == domain.PriorityHigh
	for _, e := range s.execs.snapshot() {
		if e.CanHandle("") {
			return true
		}
		if preemptible && e.HasPreemptibleTask() {
			return true
		}
	}
	return false
}

// choosePriority applies strict priority with the weighted-fairness override.
func (s *Scheduler) choosePriority(depths map[domain.Priority]int64) (domain.Priority, bool) {
	// Forced low-priority opportunity: after N consecutive high pops,
	// inspect Low then Bulk first.
	if s.highStreak >= s.highWatermark {
		for i := domain.PriorityLow.Rank(); i < len(domain.PriorityOrder); i++ {
			p := domain.PriorityOrder[i]
			if s.availableFor(p, depths) {
				return p, true
			}
		}
		s.highStreak = 0 // nothing low available; fairness credit is spent
	}
	for _, p := range domain.PriorityOrder {
		if s.availableFor(p, depths) {
			return p, true
		}
	}
	return "", false
}

// notePop updates the fairness streak after a successful pop.
func (s *Scheduler) notePop(p domain.Priority) {
	if p.Rank() < domain.PriorityLow.Rank() {
		s.highStreak++
	} else {
		s.highStreak = 0
	}
}

// maybePreempt opens capacity for an incoming critical/high task by
// interrupting a strictly lower-priority running task. It performs the
// Postgres transition, cancels the executor context and pushes the victim
// back to the head of its own ready queue.
func (s *Scheduler) maybePreempt(ctx context.Context, t *domain.Task) (Executor, error) {
	type victim struct {
		exec Executor
		task *domain.Task
	}
	var best victim
	bestRank := -1
	for _, e := range s.execs.snapshot() {
		// The executor is (about to be) full; match on capability only, since
		// preempting a running task frees a slot.
		if !e.Capable(t.Type) {
			continue
		}
		for _, rt := range e.RunningTasks() {
			if rt == nil || rt.Priority.Rank() <= t.Priority.Rank() {
				continue // preempt only strictly lower priority
			}
			if bestRank == -1 || rt.Priority.Rank() > bestRank {
				best = victim{e, rt}
				bestRank = rt.Priority.Rank()
			}
		}
	}
	if best.task == nil {
		return nil, nil
	}
	if _, err := s.store.Preempt(ctx, best.task.ID, "scheduler"); err != nil {
		if errors.Is(err, storage.ErrConflict) {
			return nil, nil // victim finished/was reclaimed first
		}
		return nil, err
	}
	best.exec.Preempt(best.task.ID)
	best.exec.ReleaseSlot(best.task.ID)
	if err := s.q.EnqueueHead(ctx, best.task.Priority, best.task.ID); err != nil {
		return nil, fmt.Errorf("requeue preempted task: %w", err)
	}
	return best.exec, nil
}

// Tick performs one scheduling pass and returns how many tasks were handed
// out. It keeps dispatching until no free slot or no ready task remains.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	dispatched := 0
	for i := 0; i < s.execs.totalSlots()*2+1; i++ {
		ok, err := s.dispatchOne(ctx)
		if err != nil {
			return dispatched, err
		}
		if !ok {
			break
		}
		dispatched++
	}
	return dispatched, nil
}

func (s *Scheduler) dispatchOne(ctx context.Context) (bool, error) {
	depths, err := s.q.AllDepths(ctx)
	if err != nil {
		return false, err
	}
	prio, ok := s.choosePriority(depths)
	if !ok {
		return false, nil
	}
	id, err := s.q.Dequeue(ctx, prio)
	if err != nil || id == "" {
		return false, err
	}
	task, err := s.store.GetTask(ctx, id)
	if err != nil {
		return false, err
	}
	if task == nil {
		return true, nil // task vanished; treat as consumed
	}

	exec := s.execs.pick(task.Type)
	if exec == nil && (task.Priority == domain.PriorityCritical || task.Priority == domain.PriorityHigh) {
		freed, perr := s.maybePreempt(ctx, task)
		if perr != nil {
			_ = s.requeue(ctx, task)
			return false, perr
		}
		exec = freed
	}
	if exec == nil {
		return false, s.requeue(ctx, task)
	}

	if !exec.ReserveSlot(task.ID) {
		return false, s.requeue(ctx, task)
	}

	lease := time.Now().UTC().Add(s.leaseFor)
	claimed, err := s.store.BeginAttempt(ctx, task.ID, exec.ID(), lease)
	if err != nil {
		exec.ReleaseSlot(task.ID)
		if errors.Is(err, storage.ErrConflict) {
			return true, nil // moved on; do not requeue
		}
		_ = s.requeue(ctx, task)
		return false, err
	}
	if err := exec.Deliver(ctx, claimed); err != nil {
		exec.ReleaseSlot(task.ID)
		_ = s.requeue(ctx, claimed)
		return false, err
	}
	s.notePop(prio)
	return true, nil
}

func (s *Scheduler) requeue(ctx context.Context, t *domain.Task) error {
	return s.q.EnqueueHead(ctx, t.Priority, t.ID)
}

// Run drives Tick on an interval until ctx is canceled.
func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = s.Tick(ctx)
		}
	}
}
