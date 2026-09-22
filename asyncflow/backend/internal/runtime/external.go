package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/config"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/scheduler"
	"github.com/asyncflow/engine/internal/storage"
)

// ExternalWorker is a scheduler.Executor backed by a remote process that
// long-polls for tasks and reports results over HTTP.
type ExternalWorker struct {
	id    string
	mgr   *ExternalManager
	caps  map[string]bool
	slots int

	mu      sync.Mutex
	used    map[string]bool // reserved/claimed task ids
	pending chan *domain.Task
	preempt map[string]bool
	online  bool
}

func newExternalWorker(mgr *ExternalManager, id string, caps []string, slots int) *ExternalWorker {
	c := map[string]bool{}
	for _, x := range caps {
		c[x] = true
	}
	return &ExternalWorker{
		id: id, mgr: mgr, caps: c, slots: slots,
		used:    map[string]bool{},
		pending: make(chan *domain.Task, slots*2+4),
		preempt: map[string]bool{},
		online:  true,
	}
}

func (w *ExternalWorker) ID() string { return w.id }

func (w *ExternalWorker) CanHandle(taskType string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.online || len(w.used) >= w.slots {
		return false
	}
	if taskType == "" || w.caps["*"] {
		return true
	}
	return w.caps[taskType]
}

func (w *ExternalWorker) Capable(taskType string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.online {
		return false
	}
	return taskType == "" || w.caps["*"] || w.caps[taskType]
}

func (w *ExternalWorker) HasPreemptibleTask() bool {
	for _, t := range w.RunningTasks() {
		if t != nil && t.Priority.Rank() >= domain.PriorityNormal.Rank() {
			return true
		}
	}
	return false
}

func (w *ExternalWorker) ReserveSlot(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.online || len(w.used) >= w.slots {
		return false
	}
	if w.used[taskID] {
		return false
	}
	w.used[taskID] = true
	return true
}

func (w *ExternalWorker) ReleaseSlot(taskID string) {
	w.mu.Lock()
	delete(w.used, taskID)
	w.mu.Unlock()
}

func (w *ExternalWorker) Deliver(ctx context.Context, t *domain.Task) error {
	w.mu.Lock()
	online := w.online
	w.mu.Unlock()
	if !online {
		return errWorkerGone
	}
	select {
	case w.pending <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errBufferFull
	}
}

func (w *ExternalWorker) Preempt(taskID string) {
	w.mu.Lock()
	w.preempt[taskID] = true
	w.mu.Unlock()
}

func (w *ExternalWorker) takePreempt(taskID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	v := w.preempt[taskID]
	delete(w.preempt, taskID)
	return v
}

func (w *ExternalWorker) RunningTasks() map[string]*domain.Task {
	return w.mgr.snapshotRunning(w.id)
}

// PollClaim blocks (up to wait) for a dispatched task.
func (w *ExternalWorker) PollClaim(ctx context.Context, wait time.Duration) *domain.Task {
	select {
	case t := <-w.pending:
		return t
	case <-time.After(wait):
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (w *ExternalWorker) markOffline() {
	w.mu.Lock()
	w.online = false
	close(w.pending)
	w.mu.Unlock()
}

// ExternalManager tracks all registered external workers.
type ExternalManager struct {
	store *storage.Store
	q     *queue.RedisQueue
	cfg   config.Config

	mu      sync.RWMutex
	workers map[string]*ExternalWorker
	running map[string]map[string]*domain.Task // workerID -> taskID -> task

	OnRegister func(ex *ExternalWorker)
	OnRemove   func(id string)
}

func NewExternalManager(store *storage.Store, q *queue.RedisQueue, cfg config.Config) *ExternalManager {
	return &ExternalManager{
		store: store, q: q, cfg: cfg,
		workers: map[string]*ExternalWorker{},
		running: map[string]map[string]*domain.Task{},
	}
}

var (
	errWorkerGone = &simpleErr{"worker is offline"}
	errBufferFull = &simpleErr{"worker claim buffer full"}
)

type simpleErr struct{ msg string }

func (e *simpleErr) Error() string { return e.msg }

// Register creates or refreshes an external worker.
func (m *ExternalManager) Register(ctx context.Context, id, name string, caps []string, slots int) (ClaimHandle, error) {
	m.mu.Lock()
	w, ok := m.workers[id]
	if !ok {
		w = newExternalWorker(m, id, caps, slots)
		m.workers[id] = w
		m.running[id] = map[string]*domain.Task{}
	} else {
		w.mu.Lock()
		for _, c := range caps {
			w.caps[c] = true
		}
		if slots > 0 {
			w.slots = slots
		}
		w.online = true
		w.mu.Unlock()
	}
	cb := m.OnRegister
	m.mu.Unlock()

	if err := m.store.UpsertWorker(ctx, &domain.Worker{
		ID: id, Name: name, Capabilities: caps,
		TotalSlots: slots, Status: "online",
	}); err != nil {
		return nil, err
	}
	if cb != nil {
		cb(w)
	}
	return w, nil
}

// ClaimHandle is the interface returned to the API for long-poll claiming.
type ClaimHandle interface {
	ID() string
	PollClaim(ctx context.Context, wait time.Duration) *domain.Task
	ReleaseSlot(taskID string)
}

func (m *ExternalManager) Get(id string) ClaimHandle {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.workers[id]
}

// Heartbeat persists liveness and tracks running tasks.
func (m *ExternalManager) Heartbeat(ctx context.Context, id string, taskIDs []string) error {
	m.mu.RLock()
	w := m.workers[id]
	m.mu.RUnlock()
	if w == nil {
		return errWorkerGone
	}
	used := len(taskIDs)
	domainW := &domain.Worker{
		ID: id, UsedSlots: used, TotalSlots: w.slots,
		CurrentTaskIDs: taskIDs, Status: "online",
	}
	if err := m.store.Heartbeat(ctx, domainW); err != nil {
		return err
	}
	m.mu.Lock()
	if m.running[id] == nil {
		m.running[id] = map[string]*domain.Task{}
	}
	m.mu.Unlock()
	return nil
}

// TaskStarted records a task the remote worker is executing.
func (m *ExternalManager) TaskStarted(workerID string, t *domain.Task) {
	m.mu.Lock()
	if m.running[workerID] == nil {
		m.running[workerID] = map[string]*domain.Task{}
	}
	m.running[workerID][t.ID] = t
	m.mu.Unlock()
}

// TaskFinished removes a task from running tracking.
func (m *ExternalManager) TaskFinished(workerID, taskID string) {
	m.mu.Lock()
	if set := m.running[workerID]; set != nil {
		delete(set, taskID)
	}
	m.mu.Unlock()
}

func (m *ExternalManager) snapshotRunning(workerID string) map[string]*domain.Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]*domain.Task{}
	for id, t := range m.running[workerID] {
		out[id] = t
	}
	return out
}

// SnapshotIDs returns the running task ids tracked for a worker.
func (m *ExternalManager) SnapshotIDs(workerID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for id := range m.running[workerID] {
		out = append(out, id)
	}
	return out
}

// WorkerOffline satisfies reaper.WorkerLivenessNotifier.
func (m *ExternalManager) WorkerOffline(ctx context.Context, workerID string) {
	m.mu.Lock()
	w := m.workers[workerID]
	delete(m.running, workerID)
	m.mu.Unlock()
	if w != nil {
		w.markOffline()
	}
	if m.OnRemove != nil {
		m.OnRemove(workerID)
	}
	m.mu.Lock()
	delete(m.workers, workerID)
	m.mu.Unlock()
}

// RunHeartbeatGC is a no-op safety net; the authoritative liveness check is
// the reaper loop. It exists so external worker churn is always reconciled.
func (m *ExternalManager) RunHeartbeatGC(ctx context.Context, ttlSecs int) {
	<-ctx.Done()
}

var _ scheduler.Executor = (*ExternalWorker)(nil)
