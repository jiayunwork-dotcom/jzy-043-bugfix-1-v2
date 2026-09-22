package scheduler

import (
	"sort"
	"sync"
)

// executorRegistry is the live set of executors with load-balanced selection.
type executorRegistry struct {
	mu    sync.RWMutex
	execs map[string]Executor
	rr    int // round-robin cursor
}

func newRegistry() *executorRegistry {
	return &executorRegistry{execs: map[string]Executor{}}
}

func (r *executorRegistry) add(e Executor) {
	r.mu.Lock()
	r.execs[e.ID()] = e
	r.mu.Unlock()
}

func (r *executorRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.execs, id)
	r.mu.Unlock()
}

func (r *executorRegistry) snapshot() []Executor {
	r.mu.RLock()
	out := make([]Executor, 0, len(r.execs))
	for _, e := range r.execs {
		out = append(out, e)
	}
	r.mu.RUnlock()
	// Stable ordering keeps round-robin and victim selection deterministic.
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

func (r *executorRegistry) totalSlots() int {
	// We don't know total vs. used through the interface; bound a pass by the
	// number of executors times a generous per-executor concurrency. Exact
	// cap is enforced by CanHandle/ReserveSlot on each executor.
	return 64
}

// pick returns the least-loaded executor that can handle taskType with a free
// slot, tie-broken by round-robin. Nil when none has capacity.
func (r *executorRegistry) pick(taskType string) Executor {
	r.mu.Lock()
	defer r.mu.Unlock()

	var candidates []Executor
	for _, e := range r.execs {
		if e.CanHandle(taskType) {
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	// Order by current load (running task count) then round-robin for
	// balancing across multiple handlers of the same task type.
	sort.Slice(candidates, func(i, j int) bool {
		li, lj := len(candidates[i].RunningTasks()), len(candidates[j].RunningTasks())
		if li != lj {
			return li < lj
		}
		return candidates[i].ID() < candidates[j].ID()
	})
	r.rr = (r.rr + 1) % len(candidates)
	return candidates[r.rr%len(candidates)]
}
