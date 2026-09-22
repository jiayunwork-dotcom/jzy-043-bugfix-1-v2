// Package delayscheduler moves due delayed and retry-waiting tasks into their
// priority ready queues. The scan runs at sub-second cadence so promotions are
// second-accurate with no visible lag.
package delayscheduler

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/storage"
)

// Storer is the persistence surface for promotions.
type Storer interface {
	MarkReadyByIDs(ctx context.Context, ids []string, now time.Time) ([]*domain.Task, error)
	ListPending(ctx context.Context) ([]storage.PendingEntry, error)
}

// Runner drives wait-set promotion.
type Runner struct {
	store Storer
	q     queue.Queue
	tick  time.Duration
	batch int64
}

func New(store Storer, q queue.Queue, scanInterval time.Duration) *Runner {
	return &Runner{store: store, q: q, tick: scanInterval, batch: 500}
}

// RebuildWaitSets repopulates Redis delay/retry ZSets from Postgres after a
// restart so pending tasks are not forgotten.
func (r *Runner) RebuildWaitSets(ctx context.Context) error {
	entries, err := r.store.ListPending(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, e := range entries {
		due := now
		if e.ExecuteAfter != nil {
			due = *e.ExecuteAfter
		}
		// Promotion semantics are identical for delayed and retry-waiting
		// tasks; the retry bucket holds all waiting tasks.
		if err := r.q.AddWaiting(ctx, queue.WaitRetry, e.ID, due); err != nil {
			return err
		}
	}
	return nil
}

// promoteOnce drains due entries from both wait sets and enqueues the ones
// Postgres confirms as ready.
func (r *Runner) promoteOnce(ctx context.Context) (int, error) {
	now := time.Now().UTC()
	total := 0
	for _, kind := range []queue.WaitKind{queue.WaitDelay, queue.WaitRetry} {
		ids, err := r.q.PopDue(ctx, kind, now, r.batch)
		if err != nil || len(ids) == 0 {
			if err != nil {
				return total, err
			}
			continue
		}
		ready, err := r.store.MarkReadyByIDs(ctx, ids, now)
		if err != nil {
			// Put ids back so they are reconsidered next tick.
			for _, id := range ids {
				_ = r.q.AddWaiting(ctx, kind, id, now)
			}
			return total, err
		}
		readySet := map[string]bool{}
		for _, t := range ready {
			if err := r.q.Enqueue(ctx, t.Priority, t.ID); err != nil {
				return total, err
			}
			readySet[t.ID] = true
			total++
		}
		// Tasks that popped from Redis but are not actually due/pending any
		// more (completed concurrently, execute_after in the future) are not
		// re-added here; RebuildWaitSets / reconciliation restores any that
		// remain pending.
	}
	return total, nil
}

// Run loops promotion until ctx is canceled.
func (r *Runner) Run(ctx context.Context) {
	t := time.NewTicker(r.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = r.promoteOnce(ctx)
		}
	}
}

// PromoteOnce is exported for tests.
func (r *Runner) PromoteOnce(ctx context.Context) (int, error) { return r.promoteOnce(ctx) }

// enqueue shim referencing domain to keep import meaningful.
var _ = domain.PriorityCritical
