package reaper_test

import (
	"context"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/reaper"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

type notifier struct{ offline []string }

func (n *notifier) WorkerOffline(_ context.Context, id string) {
	n.offline = append(n.offline, id)
}

// TestWorkerLostReclaimsInflight: when a worker stops heartbeating, its
// running task is reclaimed to ready and pushed back to its queue head; no
// task is lost.
func TestWorkerLostReclaimsInflight(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	notif := &notifier{}

	ttl := 5 * time.Second
	r := reaper.New(fs, mq, notif, ttl)

	// Register a worker whose heartbeat is already stale.
	require.NoError(t, fs.UpsertWorker(ctx, &domain.Worker{
		ID: "lost-worker", Name: "lost", TotalSlots: 2,
		LastHeartbeat: time.Now().UTC().Add(-30 * time.Second),
	}))

	task := &domain.Task{
		ID: "t-lost", Type: "sample.echo", Priority: domain.PriorityHigh,
		Status: domain.StatusReady, WorkerID: "", TimeoutSeconds: 10,
	}
	require.NoError(t, fs.CreateTask(ctx, task))
	running, err := fs.BeginAttempt(ctx, task.ID, "lost-worker", time.Now().UTC().Add(-1*time.Second))
	require.NoError(t, err)
	require.Equal(t, domain.StatusRunning, running.Status)

	n, err := r.ReapOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	reclaimed, err := fs.GetTask(ctx, task.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusReady, reclaimed.Status, "task returns to ready")
	require.Empty(t, reclaimed.WorkerID)

	require.Equal(t, "offline", fs.SnapshotWorkerStatus("lost-worker"))
	require.Contains(t, notif.offline, "lost-worker")

	// Re-enqueued to the head of its priority queue.
	id, err := mq.Dequeue(ctx, domain.PriorityHigh)
	require.NoError(t, err)
	require.Equal(t, task.ID, id)
}

// TestLeaseExpiryReclaim: a live worker whose task lease nonetheless expired
// has that task reclaimed (no heartbeat required).
func TestLeaseExpiryReclaim(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	r := reaper.New(fs, mq, nil, time.Minute)

	require.NoError(t, fs.UpsertWorker(ctx, &domain.Worker{
		ID: "live", Name: "live", TotalSlots: 2,
		LastHeartbeat: time.Now().UTC(), // fresh heartbeat
	}))
	task := &domain.Task{
		ID: "t-lease", Type: "sample.echo", Priority: domain.PriorityNormal,
		Status: domain.StatusReady, TimeoutSeconds: 10,
	}
	require.NoError(t, fs.CreateTask(ctx, task))
	_, err := fs.BeginAttempt(ctx, task.ID, "live", time.Now().UTC().Add(-10*time.Second))
	require.NoError(t, err)

	n, err := r.ReapOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, domain.StatusReady, fs.SnapshotStatus(task.ID))
}
