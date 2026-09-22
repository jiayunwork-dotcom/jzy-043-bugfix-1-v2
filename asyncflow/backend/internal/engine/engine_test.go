package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

// runOneAttempt leases a task and reports a failed attempt through the engine.
func runOneAttempt(t *testing.T, ctx context.Context, eng *engine.Engine, fs *testutil.FakeStore, id string) {
	t.Helper()
	leased, err := fs.BeginAttempt(ctx, id, "w1", time.Now().Add(30*time.Second))
	require.NoError(t, err)
	eng.ReportResult(ctx, leased, false, nil, "boom: connection refused", "external_dependency")
}

// TestRetryExhaustionEntersDeadLetter: a task with MaxRetries=2 retries after
// attempts 1 and 2, and moves to the dead-letter area after attempt 3.
func TestRetryExhaustionEntersDeadLetter(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	eng := engine.New(fs, mq, nil, nil)

	task, _, err := eng.Submit(ctx, engine.SubmitInput{
		Type: "sample.echo", Priority: domain.PriorityNormal,
		MaxRetries: 2, TimeoutSeconds: 10,
		RetryPolicy: domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 0, MaxRetries: 2},
	})
	require.NoError(t, err)
	require.Equal(t, domain.StatusReady, task.Status)

	// Attempt 1 fails -> scheduled retry (pending, in retry wait set).
	runOneAttempt(t, ctx, eng, fs, task.ID)
	require.Equal(t, domain.StatusPending, fs.SnapshotStatus(task.ID))
	w, _ := mq.WaitingDepth(ctx, queue.WaitRetry)
	require.Equal(t, int64(1), w)

	// Simulate the delay scheduler promoting it, then attempt 2 fails -> retry.
	_, err = fs.MarkReadyByIDs(ctx, []string{task.ID}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	runOneAttempt(t, ctx, eng, fs, task.ID)
	require.Equal(t, domain.StatusPending, fs.SnapshotStatus(task.ID))

	// Attempt 3 fails -> dead letter.
	_, err = fs.MarkReadyByIDs(ctx, []string{task.ID}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	runOneAttempt(t, ctx, eng, fs, task.ID)
	require.Equal(t, domain.StatusDead, fs.SnapshotStatus(task.ID))

	dead, err := fs.CountDead(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, dead)

	// Every failed attempt carries its error for the DLQ detail view.
	attempts, err := fs.Attempts(ctx, task.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 3)
	for _, a := range attempts {
		require.Equal(t, "failed", a.Status)
		require.NotEmpty(t, a.Error)
	}

	// Category aggregation works.
	agg, err := fs.AggregateDead(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, agg)
	require.Equal(t, "external_dependency", agg[0].Category)
}

// TestDeadBatchRetryReEnqueues: manually retrying dead tasks moves them back
// to ready and pushes them into their priority queues.
func TestDeadBatchRetryReEnqueues(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	eng := engine.New(fs, mq, nil, nil)

	var ids []string
	for i := 0; i < 3; i++ {
		task, _, err := eng.Submit(ctx, engine.SubmitInput{
			Type: "sample.echo", Priority: domain.PriorityLow,
			MaxRetries: 0, TimeoutSeconds: 10,
			RetryPolicy: domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 0, MaxRetries: 0},
		})
		require.NoError(t, err)
		runOneAttempt(t, ctx, eng, fs, task.ID)
		require.Equal(t, domain.StatusDead, fs.SnapshotStatus(task.ID))
		ids = append(ids, task.ID)
	}

	moved, n, err := fs.RetryDead(ctx, ids)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	for _, mt := range moved {
		require.Equal(t, domain.StatusReady, mt.Status)
		require.NoError(t, mq.Enqueue(ctx, mt.Priority, mt.ID))
	}
	for _, id := range ids {
		got, err := mq.Dequeue(ctx, domain.PriorityLow)
		require.NoError(t, err)
		require.Equal(t, id, got)
	}
}

// TestExponentialBackoffTiming checks the base*2^(n-1) schedule.
func TestExponentialBackoffTiming(t *testing.T) {
	p := domain.RetryPolicy{Kind: domain.RetryExponential, BaseInterval: 2}
	d1, err := p.NextDelay(1, time.Now())
	require.NoError(t, err)
	require.Equal(t, 2, d1)
	d2, err := p.NextDelay(2, time.Now())
	require.NoError(t, err)
	require.Equal(t, 4, d2)
	d3, err := p.NextDelay(3, time.Now())
	require.NoError(t, err)
	require.Equal(t, 8, d3)
}

// TestIdempotentSubmit: same idempotency key returns the existing task and
// does not create a duplicate.
func TestIdempotentSubmit(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	eng := engine.New(fs, mq, nil, nil)

	in := engine.SubmitInput{
		Type: "t", Priority: domain.PriorityNormal, TimeoutSeconds: 5,
		IDempotencyKey: "key-123",
	}
	first, dup, err := eng.Submit(ctx, in)
	require.NoError(t, err)
	require.False(t, dup)
	second, dup, err := eng.Submit(ctx, in)
	require.NoError(t, err)
	require.True(t, dup)
	require.Equal(t, first.ID, second.ID)
}
