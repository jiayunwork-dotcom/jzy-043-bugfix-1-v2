package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// FakeExec is a controllable scheduler.Executor for tests.
type FakeExec struct {
	mu        sync.Mutex
	id        string
	slots     int
	used      map[string]bool
	running   map[string]*domain.Task
	delivered []string
	preempted []string
	caps      map[string]bool
	online    bool
}

func NewFakeExec(id string, slots int, caps ...string) *FakeExec {
	c := map[string]bool{}
	for _, x := range caps {
		c[x] = true
	}
	return &FakeExec{
		id: id, slots: slots, used: map[string]bool{},
		running: map[string]*domain.Task{}, caps: c, online: true,
	}
}

func (f *FakeExec) ID() string { return f.id }

func (f *FakeExec) CanHandle(typ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.online || len(f.used) >= f.slots {
		return false
	}
	if typ == "" || f.caps["*"] {
		return true
	}
	return f.caps[typ]
}

func (f *FakeExec) Capable(typ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.online {
		return false
	}
	return typ == "" || f.caps["*"] || f.caps[typ]
}

func (f *FakeExec) HasPreemptibleTask() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.running {
		if t != nil && t.Priority.Rank() >= domain.PriorityNormal.Rank() {
			return true
		}
	}
	return false
}

func (f *FakeExec) ReserveSlot(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.online || len(f.used) >= f.slots || f.used[id] {
		return false
	}
	f.used[id] = true
	return true
}

func (f *FakeExec) ReleaseSlot(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.used, id)
	delete(f.running, id)
}

func (f *FakeExec) Deliver(_ context.Context, t *domain.Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, t.ID)
	f.running[t.ID] = t
	return nil
}

func (f *FakeExec) RunningTasks() map[string]*domain.Task {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]*domain.Task{}
	for k, v := range f.running {
		out[k] = v
	}
	return out
}

func (f *FakeExec) Preempt(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preempted = append(f.preempted, id)
}

// Test inspection helpers.

func (f *FakeExec) Delivered() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.delivered...)
}

func (f *FakeExec) PreemptedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.preempted...)
}

func (f *FakeExec) SetRunning(t *domain.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running[t.ID] = t
}

func (f *FakeExec) SetOnline(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.online = v
}

func (f *FakeExec) FreeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots - len(f.used)
}

var _ = time.Second
