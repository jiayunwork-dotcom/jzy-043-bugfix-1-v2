// Package testutil provides an in-memory fake of the Postgres store that
// mirrors its compare-and-swap state-transition semantics. It lets the
// scheduling/fault-tolerance tests run without a database while still
// exercising the exact guards that prevent lost or duplicate execution.
package testutil

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/storage"
)

// FakeStore is a goroutine-safe in-memory store.
type FakeStore struct {
	mu          sync.Mutex
	Tasks       map[string]*domain.Task
	Idem        map[string]string // idempotency key -> task id
	attemptRows map[string][]domain.Attempt
	Workers     map[string]*domain.Worker
	DAGs        map[string]*domain.DAG
	Nodes       map[string]map[string]*domain.DAGNode // dagID -> nodeID -> node
	Audit       []string
	auditRows   []map[string]any
	nextAttempt int64
}

func NewFakeStore() *FakeStore {
	return &FakeStore{
		Tasks:       map[string]*domain.Task{},
		Idem:        map[string]string{},
		attemptRows: map[string][]domain.Attempt{},
		Workers:     map[string]*domain.Worker{},
		DAGs:        map[string]*domain.DAG{},
		Nodes:       map[string]map[string]*domain.DAGNode{},
	}
}

func clone(t *domain.Task) *domain.Task {
	c := *t
	return &c
}

func (s *FakeStore) record(entity, entityID, action, from, to, actor, detail string) {
	s.auditRows = append(s.auditRows, map[string]any{
		"entity": entity, "entity_id": entityID, "action": action,
		"from_state": from, "to_state": to, "actor": actor, "detail": detail,
		"created_at": time.Now().UTC(),
	})
}

// ---- Tasks ----

func (s *FakeStore) CreateTask(_ context.Context, t *domain.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.IDempotencyKey != "" {
		if _, exists := s.Idem[t.IDempotencyKey]; exists {
			return storage.ErrConflict
		}
		s.Idem[t.IDempotencyKey] = t.ID
	}
	if _, exists := s.Tasks[t.ID]; exists {
		return storage.ErrConflict
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now().UTC()
	}
	t.UpdatedAt = t.CreatedAt
	s.Tasks[t.ID] = clone(t)
	s.record("task", t.ID, "created", "", string(t.Status), "api", t.Type)
	return nil
}

func (s *FakeStore) GetTask(_ context.Context, id string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Tasks[id]
	if !ok {
		return nil, nil
	}
	return clone(t), nil
}

func (s *FakeStore) GetTaskByIdem(_ context.Context, key string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.Idem[key]
	if !ok {
		return nil, nil
	}
	return clone(s.Tasks[id]), nil
}

// ---- CAS transitions ----

func (s *FakeStore) BeginAttempt(_ context.Context, taskID, workerID string, lease time.Time) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Tasks[taskID]
	if !ok || t.Status != domain.StatusReady {
		return nil, storage.ErrConflict
	}
	t.Status = domain.StatusRunning
	t.WorkerID = workerID
	t.LeaseExpiresAt = &lease
	t.Attempts++
	now := time.Now().UTC()
	if t.StartedAt == nil {
		t.StartedAt = &now
	}
	t.UpdatedAt = now
	s.attemptRows[taskID] = append(s.attemptRows[taskID], domain.Attempt{
		TaskID: taskID, AttemptNo: t.Attempts, WorkerID: workerID,
		StartedAt: now, Status: "running",
	})
	s.record("task", taskID, "leased", "ready", "running", workerID, "")
	return clone(t), nil
}

func (s *FakeStore) RenewLease(_ context.Context, taskID, workerID string, expiry time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.Tasks[taskID]
	if t == nil || t.Status != domain.StatusRunning || t.WorkerID != workerID {
		return false, nil
	}
	t.LeaseExpiresAt = &expiry
	return true, nil
}

func (s *FakeStore) CompleteAttempt(_ context.Context, in storage.CompleteResult) (*domain.Task, storage.CompletionOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Tasks[in.TaskID]
	if !ok {
		return nil, "", storage.ErrConflict
	}
	if t.Status != domain.StatusRunning || t.WorkerID != in.WorkerID {
		return nil, "", storage.ErrConflict
	}
	now := time.Now().UTC()
	attempts := s.attemptRows[in.TaskID]
	for i := len(attempts) - 1; i >= 0; i-- {
		if attempts[i].EndedAt == nil {
			status := "failed"
			if in.Success {
				status = "succeeded"
			}
			attempt := &attempts[i]
			attempt.EndedAt = &now
			attempt.Status = status
			attempt.Error = in.Error
			attempt.ErrCategory = in.Category
			attempt.Result = in.Result
			break
		}
	}

	var outcome storage.CompletionOutcome
	switch {
	case in.Success:
		outcome = storage.OutcomeSucceeded
		t.Status = domain.StatusSucceeded
		fin := now
		t.FinishedAt = &fin
		s.record("task", in.TaskID, "succeeded", "running", "succeeded", in.WorkerID, "")
	case !in.RetryAfter.IsZero():
		outcome = storage.OutcomeRetrying
		t.Status = domain.StatusPending
		t.ExecuteAfter = &in.RetryAfter
		s.record("task", in.TaskID, "retry_scheduled", "running", "pending", in.WorkerID, "")
	default:
		outcome = storage.OutcomeDead
		t.Status = domain.StatusDead
		fin := now
		t.FinishedAt = &fin
		s.record("task", in.TaskID, "dead_letter", "running", "dead", in.WorkerID, in.Category)
	}
	t.LastError = in.Error
	t.ErrCategory = in.Category
	t.WorkerID = ""
	t.LeaseExpiresAt = nil
	t.UpdatedAt = now
	return clone(t), outcome, nil
}

func (s *FakeStore) Preempt(_ context.Context, taskID, _ string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Tasks[taskID]
	if !ok || t.Status != domain.StatusRunning {
		return nil, storage.ErrConflict
	}
	t.Status = domain.StatusReady
	t.WorkerID = ""
	t.LeaseExpiresAt = nil
	t.UpdatedAt = time.Now().UTC()
	now := time.Now().UTC()
	attempts := s.attemptRows[taskID]
	for i := len(attempts) - 1; i >= 0; i-- {
		if attempts[i].EndedAt == nil {
			attempts[i].EndedAt = &now
			attempts[i].Status = "interrupted"
			attempts[i].Error = "preempted"
			attempts[i].ErrCategory = "preempted"
			break
		}
	}
	return clone(t), nil
}

// ---- Waiting / reclaim ----

func (s *FakeStore) MarkReadyByIDs(_ context.Context, ids []string, now time.Time) ([]*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Task
	for _, id := range ids {
		t := s.Tasks[id]
		if t == nil || t.Status != domain.StatusPending {
			continue
		}
		if t.ExecuteAfter != nil && t.ExecuteAfter.After(now) {
			continue
		}
		t.Status = domain.StatusReady
		t.UpdatedAt = now
		out = append(out, clone(t))
	}
	return out, nil
}

type PendingEntry = storage.PendingEntry

func (s *FakeStore) ListPending(_ context.Context) ([]storage.PendingEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storage.PendingEntry
	for _, t := range s.Tasks {
		if t.Status == domain.StatusPending {
			out = append(out, storage.PendingEntry{ID: t.ID, ExecuteAfter: t.ExecuteAfter})
		}
	}
	return out, nil
}

func (s *FakeStore) MarkWorkersOffline(_ context.Context, cutoff time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for id, w := range s.Workers {
		if w.Status != "offline" && w.LastHeartbeat.Before(cutoff) {
			w.Status = "offline"
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *FakeStore) ReclaimExpired(_ context.Context, now time.Time) ([]storage.ReclaimedTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []storage.ReclaimedTask
	ids := make([]string, 0)
	for id, t := range s.Tasks {
		if t.Status != domain.StatusRunning {
			continue
		}
		expired := t.LeaseExpiresAt != nil && t.LeaseExpiresAt.Before(now)
		offline := false
		if w, ok := s.Workers[t.WorkerID]; ok && w.Status == "offline" {
			offline = true
		} else if _, ok := s.Workers[t.WorkerID]; !ok && t.WorkerID != "" {
			offline = true
		}
		if expired || offline {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		t := s.Tasks[id]
		old := t.WorkerID
		reason := "lease_expired"
		if w, ok := s.Workers[old]; ok && w.Status == "offline" {
			reason = "worker_offline"
		}
		t.Status = domain.StatusReady
		t.WorkerID = ""
		t.LeaseExpiresAt = nil
		t.UpdatedAt = now
		ts := time.Now().UTC()
		attempts := s.attemptRows[id]
		for i := len(attempts) - 1; i >= 0; i-- {
			if attempts[i].EndedAt == nil {
				attempts[i].EndedAt = &ts
				attempts[i].Status = "timeout"
				attempts[i].Error = "lease expired / worker lost"
				attempts[i].ErrCategory = "timeout"
				break
			}
		}
		out = append(out, storage.ReclaimedTask{Task: clone(t), OldWorker: old, Reason: reason})
	}
	return out, nil
}

// ---- Workers ----

func (s *FakeStore) UpsertWorker(_ context.Context, w *domain.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.Workers[w.ID]; ok {
		c.TotalSlots = w.TotalSlots
		c.Capabilities = w.Capabilities
		if c.Status == "offline" {
			c.Status = "online"
		}
		if w.LastHeartbeat.After(c.LastHeartbeat) {
			c.LastHeartbeat = w.LastHeartbeat
		}
		return nil
	}
	nw := *w
	if nw.LastHeartbeat.IsZero() {
		nw.LastHeartbeat = time.Now().UTC()
	}
	s.Workers[w.ID] = &nw
	return nil
}

func (s *FakeStore) Heartbeat(_ context.Context, w *domain.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.Workers[w.ID]
	if !ok {
		nw := *w
		nw.LastHeartbeat = time.Now().UTC()
		s.Workers[w.ID] = &nw
		return nil
	}
	cur.LastHeartbeat = time.Now().UTC()
	cur.UsedSlots = w.UsedSlots
	cur.CurrentTaskIDs = w.CurrentTaskIDs
	cur.Status = "online"
	return nil
}

func (s *FakeStore) MarkWorkerDraining(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.Workers[id]; ok {
		w.Status = "draining"
	}
	return nil
}

func (s *FakeStore) ListWorkers(_ context.Context) ([]*domain.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Worker
	for _, w := range s.Workers {
		c := *w
		out = append(out, &c)
	}
	return out, nil
}

func (s *FakeStore) IncWorkerStats(_ context.Context, id string, completed, failed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.Workers[id]; ok {
		if completed {
			w.Completed++
		}
		if failed {
			w.Failed++
		}
	}
	return nil
}

// ---- Dead letter ----

func (s *FakeStore) RetryDead(_ context.Context, ids []string) ([]*domain.Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var moved []*domain.Task
	for _, id := range ids {
		t := s.Tasks[id]
		if t == nil || t.Status != domain.StatusDead {
			continue
		}
		t.Status = domain.StatusReady
		t.LastError = ""
		t.ErrCategory = ""
		t.FinishedAt = nil
		t.ExecuteAfter = nil
		t.UpdatedAt = time.Now().UTC()
		moved = append(moved, clone(t))
	}
	return moved, len(moved), nil
}

func (s *FakeStore) DiscardDead(_ context.Context, ids []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, id := range ids {
		t := s.Tasks[id]
		if t != nil && t.Status == domain.StatusDead {
			t.Status = domain.StatusCanceled
			n++
		}
	}
	return n, nil
}

func (s *FakeStore) CountDead(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.Tasks {
		if t.Status == domain.StatusDead {
			n++
		}
	}
	return n, nil
}

func (s *FakeStore) ListDead(_ context.Context, _, _ int) ([]*domain.DeadLetter, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.DeadLetter
	n := 0
	for _, t := range s.Tasks {
		if t.Status == domain.StatusDead {
			n++
			out = append(out, &domain.DeadLetter{
				Task:     *clone(t),
				Attempts: append([]domain.Attempt(nil), s.attemptRows[t.ID]...),
				DeadAt:   t.UpdatedAt,
			})
		}
	}
	return out, n, nil
}

func (s *FakeStore) AggregateDead(_ context.Context) ([]storage.DeadAggregateRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buckets := map[string]int{}
	for _, t := range s.Tasks {
		if t.Status == domain.StatusDead {
			cat := t.ErrCategory
			if cat == "" {
				cat = "unknown"
			}
			buckets[cat]++
		}
	}
	var out []storage.DeadAggregateRow
	for cat, n := range buckets {
		out = append(out, storage.DeadAggregateRow{Category: cat, Count: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Count > out[j].Count })
	return out, nil
}

func (s *FakeStore) CancelTask(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.Tasks[id]
	if !ok {
		return storage.ErrConflict
	}
	if domain.TerminalStatuses[t.Status] {
		return storage.ErrConflict
	}
	t.Status = domain.StatusCanceled
	return nil
}

func (s *FakeStore) Attempts(_ context.Context, taskID string) ([]domain.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]domain.Attempt(nil), s.attemptRows[taskID]...)
	return out, nil
}

// ---- DAG ----

func (s *FakeStore) CreateDAG(_ context.Context, d *domain.DAG) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.DAGs[d.ID]; ok {
		return fmt.Errorf("dag exists")
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now().UTC()
	}
	s.DAGs[d.ID] = d
	s.Nodes[d.ID] = map[string]*domain.DAGNode{}
	for _, n := range d.Def.Nodes {
		s.Nodes[d.ID][n.ID] = &domain.DAGNode{
			DAGID: d.ID, NodeID: n.ID, State: domain.NodeWaiting,
			Dependencies: append([]string{}, n.Dependencies...),
		}
	}
	return nil
}

func (s *FakeStore) GetDAG(_ context.Context, id string) (*domain.DAG, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.DAGs[id]
	if !ok {
		return nil, nil
	}
	c := *d
	return &c, nil
}

func (s *FakeStore) ListDAGs(_ context.Context, _ int) ([]*domain.DAG, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.DAG
	for _, d := range s.DAGs {
		c := *d
		out = append(out, &c)
	}
	return out, nil
}

func (s *FakeStore) ListDAGNodes(_ context.Context, dagID string) ([]*domain.DAGNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.DAGNode
	for _, n := range s.Nodes[dagID] {
		c := *n
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (s *FakeStore) UpdateDAGNodes(_ context.Context, dagID string, agg domain.DAGStatus, updates []storage.DAGNodeUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range updates {
		n := s.Nodes[dagID][u.NodeID]
		if n == nil {
			continue
		}
		n.State = u.State
		if u.TaskID != "" {
			n.TaskID = u.TaskID
		}
		if u.Bump {
			n.Attempts++
		}
		n.UpdatedAt = time.Now().UTC()
	}
	if agg != "" {
		if d, ok := s.DAGs[dagID]; ok {
			d.Status = agg
			if agg == domain.DAGSucceeded || agg == domain.DAGFailed {
				now := time.Now().UTC()
				d.FinishedAt = &now
			}
		}
	}
	return nil
}

func (s *FakeStore) LinkDAGNode(_ context.Context, taskID, dagID, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.Tasks[taskID]; ok {
		t.DAGID = dagID
		t.DAGNodeID = nodeID
	}
	return nil
}

// SnapshotStatus is a test helper returning a task status by id.
func (s *FakeStore) SnapshotStatus(id string) domain.TaskStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.Tasks[id]; ok {
		return t.Status
	}
	return ""
}

// ListAllTasks returns every persisted task (test helper).
func (s *FakeStore) ListAllTasks() ([]*domain.Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Task
	for _, t := range s.Tasks {
		out = append(out, clone(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, len(out), nil
}

// ListTasks mirrors the Postgres filtered listing used by the API.
func (s *FakeStore) ListTasks(_ context.Context, f storage.TaskFilter) ([]*domain.Task, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*domain.Task
	statusSet := map[string]bool{}
	for _, x := range f.Statuses {
		statusSet[x] = true
	}
	for _, t := range s.Tasks {
		if len(statusSet) > 0 && !statusSet[string(t.Status)] {
			continue
		}
		if f.Priority != "" && string(t.Priority) != f.Priority {
			continue
		}
		if f.Type != "" && t.Type != f.Type {
			continue
		}
		if f.DAGID != "" && t.DAGID != f.DAGID {
			continue
		}
		if f.From != nil && t.CreatedAt.Before(*f.From) {
			continue
		}
		if f.To != nil && t.CreatedAt.After(*f.To) {
			continue
		}
		out = append(out, clone(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	total := len(out)
	start := f.Offset
	if start > total {
		start = total
	}
	end := start + f.Limit
	if f.Limit <= 0 {
		end = total
	}
	if end > total {
		end = total
	}
	return out[start:end], total, nil
}

func (s *FakeStore) GetWorker(_ context.Context, id string) (*domain.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.Workers[id]
	if !ok {
		return nil, nil
	}
	c := *w
	return &c, nil
}

// WorkerStats feeds the metrics collector.
func (s *FakeStore) WorkerStats(_ context.Context) ([]metrics.WorkerStat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []metrics.WorkerStat
	for _, w := range s.Workers {
		out = append(out, metrics.WorkerStat{
			Online:     w.Status == "online" || w.Status == "draining",
			TotalSlots: w.TotalSlots,
			UsedSlots:  w.UsedSlots,
		})
	}
	return out, nil
}

// DeadCount implements metrics.StatsSource.
func (s *FakeStore) DeadCount(ctx context.Context) (int, error) {
	return s.CountDead(ctx)
}

// ListAudit returns recorded state changes (in-memory keeps a summary list).
func (s *FakeStore) ListAudit(_ context.Context, entity, entityID string, _ int) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, a := range s.auditRows {
		if entity != "" && a["entity"] != entity {
			continue
		}
		if entityID != "" && a["entity_id"] != entityID {
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

// SnapshotWorkerStatus is a test helper.
func (s *FakeStore) SnapshotWorkerStatus(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.Workers[id]; ok {
		return w.Status
	}
	return ""
}
