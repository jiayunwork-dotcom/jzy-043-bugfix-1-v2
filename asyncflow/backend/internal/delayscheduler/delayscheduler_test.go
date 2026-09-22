package delayscheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/delayscheduler"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

// TestDelayedTaskPromotesWithinOneSecond: a delayed task becomes ready at
// second-level precision once its execute_after elapses, entering its
// priority ready queue with no visible scheduling lag.
func TestDelayedTaskPromotesWithinOneSecond(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	runner := delayscheduler.New(fs, mq, 20*time.Millisecond)

	// One due, one not yet due.
	past := time.Now().UTC().Add(-2 * time.Second)
	future := time.Now().UTC().Add(60 * time.Second)

	dueTask := &domain.Task{
		ID: "due-1", Type: "sample.echo", Priority: domain.PriorityHigh,
		Status: domain.StatusPending, ExecuteAfter: &past, TimeoutSeconds: 10,
	}
	laterTask := &domain.Task{
		ID: "later-1", Type: "sample.echo", Priority: domain.PriorityLow,
		Status: domain.StatusPending, ExecuteAfter: &future, TimeoutSeconds: 10,
	}
	require.NoError(t, fs.CreateTask(ctx, dueTask))
	require.NoError(t, fs.CreateTask(ctx, laterTask))

	// Wait-set membership mirrors what Submit adds: score = due time.
	require.NoError(t, mq.AddWaiting(ctx, queue.WaitDelay, dueTask.ID, *dueTask.ExecuteAfter))
	require.NoError(t, mq.AddWaiting(ctx, queue.WaitDelay, laterTask.ID, *laterTask.ExecuteAfter))

	promotedAt := time.Now()
	n, err := runner.PromoteOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the due task promotes")

	// Promoted immediately (it was already due); measure that a near-future
	// task flips within ~1s of its deadline.
	require.WithinDuration(t, time.Now(), promotedAt, time.Second)

	d, err := mq.Depth(ctx, domain.PriorityHigh)
	require.NoError(t, err)
	require.Equal(t, int64(1), d, "due task enters its priority ready queue")

	id, err := mq.Dequeue(ctx, domain.PriorityHigh)
	require.NoError(t, err)
	require.Equal(t, "due-1", id)

	got, err := fs.GetTask(ctx, "due-1")
	require.NoError(t, err)
	require.Equal(t, domain.StatusReady, got.Status)

	// Future task stays pending and in the delay wait set.
	still, err := fs.GetTask(ctx, "later-1")
	require.NoError(t, err)
	require.Equal(t, domain.StatusPending, still.Status)
	w, err := mq.WaitingDepth(ctx, queue.WaitDelay)
	require.NoError(t, err)
	require.Equal(t, int64(1), w)

	// Second-level precision: schedule a task 1s out, advance the wait set by
	// scanning; it must promote on the first tick after the deadline.
	deadline := time.Now().UTC().Add(1 * time.Second)
	oneSec := &domain.Task{
		ID: "one-sec", Type: "sample.echo", Priority: domain.PriorityNormal,
		Status: domain.StatusPending, ExecuteAfter: &deadline, TimeoutSeconds: 10,
	}
	require.NoError(t, fs.CreateTask(ctx, oneSec))
	require.NoError(t, mq.AddWaiting(ctx, queue.WaitDelay, oneSec.ID, deadline))

	require.Never(t, func() bool {
		_, _ = runner.PromoteOnce(ctx)
		d, _ := mq.Depth(ctx, domain.PriorityNormal)
		return d > 0
	}, 700*time.Millisecond, 25*time.Millisecond, "must not promote before deadline")

	require.Eventually(t, func() bool {
		_, err := runner.PromoteOnce(ctx)
		return err == nil && fs.SnapshotStatus("one-sec") == domain.StatusReady
	}, 2*time.Second, 25*time.Millisecond, "promotes within ~1s after deadline")
}
