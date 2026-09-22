package workerpool

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// startLeaseRenewer periodically extends the DB lease while a task runs.
// Returns a stop function. If renewal fails (lost ownership, e.g. worker
// judged dead), the task context is canceled so execution stops promptly.
func (w *Worker) startLeaseRenewer(ctx context.Context, t *domain.Task) func() {
	if w.renewer == nil {
		return func() {}
	}
	stop := make(chan struct{})
	lease := time.Duration(w.leaseSeconds) * time.Second
	go func() {
		ticker := time.NewTicker(lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				if !w.renewer.RenewLease(ctx, t.ID, w.id, lease) {
					// Lease lost: reaper will reclaim; cancel local execution.
					w.mu.Lock()
					if it := w.tasks[t.ID]; it != nil {
						it.cancel()
					}
					w.mu.Unlock()
					return
				}
			}
		}
	}()
	return func() { close(stop) }
}
