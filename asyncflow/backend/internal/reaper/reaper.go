// Package reaper detects lost workers and expired task leases and safely
// returns their in-flight tasks to the ready queues so no work is lost or
// double-executed.
package reaper

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/storage"
)

// Storer is the persistence surface.
type Storer interface {
	MarkWorkersOffline(ctx context.Context, cutoff time.Time) ([]string, error)
	ReclaimExpired(ctx context.Context, now time.Time) ([]storage.ReclaimedTask, error)
}

// Queuer is the Redis data plane (only head requeue is needed).
type Queuer interface {
	EnqueueHead(ctx context.Context, p domain.Priority, taskID string) error
}

// WorkerLivenessNotifier lets the reaper tell the runtime to drop dead
// external executors (optional; embedded workers self-report).
type WorkerLivenessNotifier interface {
	WorkerOffline(ctx context.Context, workerID string)
}

// Reaper runs the recovery loop.
type Reaper struct {
	store        Storer
	q            Queuer
	notifier     WorkerLivenessNotifier
	heartbeatTTL time.Duration
}

func New(store Storer, q Queuer, notifier WorkerLivenessNotifier, heartbeatTTL time.Duration) *Reaper {
	return &Reaper{store: store, q: q, notifier: notifier, heartbeatTTL: heartbeatTTL}
}

// ReapOnce performs one recovery pass and returns the number of tasks
// re-enqueued.
func (r *Reaper) ReapOnce(ctx context.Context) (int, error) {
	now := time.Now().UTC()

	offline, err := r.store.MarkWorkersOffline(ctx, now.Add(-r.heartbeatTTL))
	if err != nil {
		return 0, err
	}
	if r.notifier != nil {
		for _, id := range offline {
			r.notifier.WorkerOffline(ctx, id)
		}
	}

	reclaimed, err := r.store.ReclaimExpired(ctx, now)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, rc := range reclaimed {
		if err := r.q.EnqueueHead(ctx, rc.Task.Priority, rc.Task.ID); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Run loops ReapOnce until ctx is canceled.
func (r *Reaper) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = r.ReapOnce(ctx)
		}
	}
}
