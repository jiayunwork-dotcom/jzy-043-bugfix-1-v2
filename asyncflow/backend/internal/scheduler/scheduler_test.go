package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/scheduler"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

func seedTask(t *testing.T, fs *testutil.FakeStore, id string, p domain.Priority, status domain.TaskStatus) *domain.Task {
	t.Helper()
	task := &domain.Task{
		ID: id, Type: "sample.echo", Priority: p, Status: status,
		TimeoutSeconds: 30, MaxRetries: 0,
		RetryPolicy: domain.RetryPolicy{Kind: domain.RetryExponential, BaseInterval: 1},
	}
	require.NoError(t, fs.CreateTask(context.Background(), task))
	return task
}

// TestFairnessLowPriorityNotStarved: while high-priority tasks arrive
// continuously, low-priority tasks are still consumed at the configured
// watermark rhythm.
func TestFairnessLowPriorityNotStarved(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()

	// Watermark 3 -> after 3 high pops, one low/bulk pop is forced.
	sched := scheduler.New(fs, mq, 30*time.Second, 3)
	exec := testutil.NewFakeExec("w1", 200, "*")
	sched.AddExecutor(exec)

	// 20 critical and 20 bulk queued up front.
	for i := 0; i < 20; i++ {
		id := "crit-" + itoa(i)
		seedTask(t, fs, id, domain.PriorityCritical, domain.StatusReady)
		require.NoError(t, mq.Enqueue(ctx, domain.PriorityCritical, id))
	}
	for i := 0; i < 20; i++ {
		id := "bulk-" + itoa(i)
		seedTask(t, fs, id, domain.PriorityBulk, domain.StatusReady)
		require.NoError(t, mq.Enqueue(ctx, domain.PriorityBulk, id))
	}

	n, err := sched.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 40, n, "every queued task must be dispatched exactly once")

	delivered := exec.Delivered()
	require.Len(t, delivered, 40)

	// Reconstruct priority of each delivery from the store and verify rhythm:
	// first three critical, then a forced bulk while critical still remain.
	highRun := 0
	sawBulkWhileCriticalRemained := false
	remainingCrit := 20
	for i, id := range delivered {
		task, err := fs.GetTask(ctx, id)
		require.NoError(t, err)
		switch task.Priority {
		case domain.PriorityCritical:
			remainingCrit--
			highRun++
		case domain.PriorityBulk:
			if remainingCrit > 0 {
				sawBulkWhileCriticalRemained = true
			}
			// Forced low pops must happen right after a high run of watermark.
			require.LessOrEqual(t, highRun, 3, "bulk at position %d after %d consecutive high pops", i, highRun)
			highRun = 0
		}
	}
	require.True(t, sawBulkWhileCriticalRemained,
		"a bulk task must be dispatched while critical tasks still remain (anti-starvation)")

	// Explicit first-four rhythm check: C, C, C, B.
	firstFour := delivered[:4]
	for i := 0; i < 3; i++ {
		tk, _ := fs.GetTask(ctx, firstFour[i])
		require.Equal(t, domain.PriorityCritical, tk.Priority)
	}
	fourth, _ := fs.GetTask(ctx, firstFour[3])
	require.Equal(t, domain.PriorityBulk, fourth.Priority,
		"4th dispatch must be the forced low-priority opportunity")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestCriticalPreemptionRequeuesAtHead: a running Normal task on the only
// occupied worker is interrupted by an arriving Critical task; the victim
// goes back to the head of its queue and is not lost.
func TestCriticalPreemptionRequeuesAtHead(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()

	sched := scheduler.New(fs, mq, 30*time.Second, 5)
	exec := testutil.NewFakeExec("w1", 1, "*")
	sched.AddExecutor(exec)

	// Normal task already running on the single slot.
	normal := seedTask(t, fs, "normal-1", domain.PriorityNormal, domain.StatusReady)
	require.NoError(t, mq.Enqueue(ctx, domain.PriorityNormal, normal.ID))
	running, err := fs.BeginAttempt(ctx, normal.ID, "w1", time.Now().Add(30*time.Second))
	require.NoError(t, err)
	require.True(t, exec.ReserveSlot(normal.ID))
	exec.SetRunning(running)

	// Another normal task sits behind it.
	normal2 := seedTask(t, fs, "normal-2", domain.PriorityNormal, domain.StatusReady)
	require.NoError(t, mq.Enqueue(ctx, domain.PriorityNormal, normal2.ID))

	// Critical arrives.
	crit := seedTask(t, fs, "crit-1", domain.PriorityCritical, domain.StatusReady)
	require.NoError(t, mq.Enqueue(ctx, domain.PriorityCritical, crit.ID))

	n, err := sched.Tick(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "only the critical task fits the freed slot")

	// Critical delivered to the worker.
	require.Equal(t, []string{crit.ID}, exec.Delivered())
	require.Contains(t, exec.PreemptedIDs(), normal.ID, "worker context must be canceled")

	// Victim is back to ready, at the head of the Normal queue (before normal-2).
	head, err := mq.Dequeue(ctx, domain.PriorityNormal)
	require.NoError(t, err)
	require.Equal(t, normal.ID, head, "preempted task must re-enter at queue head")
	next, err := mq.Dequeue(ctx, domain.PriorityNormal)
	require.NoError(t, err)
	require.Equal(t, normal2.ID, next)

	victim, err := fs.GetTask(ctx, normal.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusReady, victim.Status, "victim not lost and re-runnable")
	require.Empty(t, victim.WorkerID)

	// The interrupted attempt is recorded for the execution timeline.
	attempts, err := fs.Attempts(ctx, normal.ID)
	require.NoError(t, err)
	require.NotEmpty(t, attempts)
	last := attempts[len(attempts)-1]
	require.Equal(t, "interrupted", last.Status)
}

// TestNoDuplicateExecutionLeaseGuard: two dispatchers racing for the same
// ready task — exactly one wins the DB transition.
func TestNoDuplicateExecutionLeaseGuard(t *testing.T) {
	fs := testutil.NewFakeStore()
	ctx := context.Background()
	seedTask(t, fs, "t1", domain.PriorityHigh, domain.StatusReady)

	w1, err := fs.BeginAttempt(ctx, "t1", "worker-a", time.Now().Add(30*time.Second))
	require.NoError(t, err)
	require.NotNil(t, w1)

	_, err = fs.BeginAttempt(ctx, "t1", "worker-b", time.Now().Add(30*time.Second))
	require.ErrorIs(t, err, storage.ErrConflict, "second lease attempt must conflict")
}
