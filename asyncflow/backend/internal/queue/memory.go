package queue

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// MemoryQueue is a goroutine-safe in-process Queue used by unit tests.
type MemoryQueue struct {
	mu      sync.Mutex
	lists   map[domain.Priority][]string
	waiting map[WaitKind]map[string]float64 // id -> unix score
}

func NewMemoryQueue() *MemoryQueue {
	m := &MemoryQueue{
		lists:   map[domain.Priority][]string{},
		waiting: map[WaitKind]map[string]float64{},
	}
	for _, p := range domain.PriorityOrder {
		m.lists[p] = nil
	}
	for _, k := range []WaitKind{WaitDelay, WaitRetry} {
		m.waiting[k] = map[string]float64{}
	}
	return m
}

func (m *MemoryQueue) add(p domain.Priority, id string, head bool) {
	for _, x := range m.lists[p] {
		if x == id {
			return
		}
	}
	if head {
		m.lists[p] = append([]string{id}, m.lists[p]...)
	} else {
		m.lists[p] = append(m.lists[p], id)
	}
}

func (m *MemoryQueue) Enqueue(_ context.Context, p domain.Priority, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.add(p, id, false)
	return nil
}

func (m *MemoryQueue) EnqueueHead(_ context.Context, p domain.Priority, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.add(p, id, true)
	return nil
}

func (m *MemoryQueue) Dequeue(_ context.Context, p domain.Priority) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.lists[p]
	if len(l) == 0 {
		return "", nil
	}
	id := l[0]
	m.lists[p] = l[1:]
	return id, nil
}

func (m *MemoryQueue) Depth(_ context.Context, p domain.Priority) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.lists[p])), nil
}

func (m *MemoryQueue) AllDepths(_ context.Context) (map[domain.Priority]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[domain.Priority]int64{}
	for _, p := range domain.PriorityOrder {
		out[p] = int64(len(m.lists[p]))
	}
	return out, nil
}

func (m *MemoryQueue) AddWaiting(_ context.Context, kind WaitKind, id string, due time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waiting[kind][id] = float64(due.UnixMilli())
	return nil
}

func (m *MemoryQueue) RemoveWaiting(_ context.Context, kind WaitKind, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.waiting[kind], id)
	return nil
}

// RemoveWaitingKind accepts the kind as a plain string (API shim).
func (m *MemoryQueue) RemoveWaitingKind(_ context.Context, kind, id string) error {
	return m.RemoveWaiting(context.Background(), WaitKind(kind), id)
}

func (m *MemoryQueue) PopDue(_ context.Context, kind WaitKind, now time.Time, limit int64) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type kv struct {
		id    string
		score float64
	}
	var entries []kv
	for id, score := range m.waiting[kind] {
		if score <= float64(now.UnixMilli()) {
			entries = append(entries, kv{id, score})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].score < entries[j].score })
	var out []string
	for i, e := range entries {
		if int64(i) >= limit {
			break
		}
		out = append(out, e.id)
		delete(m.waiting[kind], e.id)
	}
	return out, nil
}

func (m *MemoryQueue) WaitingDepth(_ context.Context, kind WaitKind) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.waiting[kind])), nil
}

// Snapshot returns a copy of one ready queue (test helper).
func (m *MemoryQueue) Snapshot(p domain.Priority) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.lists[p]))
	copy(out, m.lists[p])
	return out
}
