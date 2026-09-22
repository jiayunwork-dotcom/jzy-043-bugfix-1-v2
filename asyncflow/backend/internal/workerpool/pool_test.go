package workerpool_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/workerpool"
	"github.com/stretchr/testify/require"
)

// fakeReporter records reported outcomes.
type fakeReporter struct {
	mu      sync.Mutex
	results []reported
}

type reported struct {
	taskID  string
	success bool
	err     string
}

func (f *fakeReporter) ReportResult(_ context.Context, t *domain.Task, success bool, _ []byte, errMsg, _ string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, reported{t.ID, success, errMsg})
}

func (f *fakeReporter) snapshot() []reported {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]reported{}, f.results...)
}

// TestPreemptionCancelsHandler: a long-running handler observes context
// cancellation when its task is preempted; completion is NOT reported (the
// scheduler owns the interrupted task state).
func TestPreemptionCancelsHandler(t *testing.T) {
	rep := &fakeReporter{}
	w := workerpool.New(workerpool.Options{
		ID: "w1", Name: "w1", Slots: 1,
		Handlers: workerpool.Handlers{
			"slow": func(ctx context.Context, _ *domain.Task) ([]byte, error) {
				select {
				case <-time.After(30 * time.Second):
					return []byte("done"), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
		Reporter:     rep,
		LeaseSeconds: 30,
	})

	task := &domain.Task{
		ID: "t1", Type: "slow", Priority: domain.PriorityNormal,
		Status: domain.StatusRunning, Attempts: 1, TimeoutSeconds: 60,
		LeaseExpiresAt: ptr(time.Now().Add(30 * time.Second)),
	}
	require.True(t, w.ReserveSlot(task.ID))
	require.NoError(t, w.Deliver(context.Background(), task))

	require.Eventually(t, func() bool { return len(w.RunningTasks()) == 1 }, time.Second, 10*time.Millisecond)

	w.Preempt(task.ID)

	// Slot released by the interrupted handler, no completion reported.
	require.Eventually(t, func() bool { return len(w.RunningTasks()) == 0 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, rep.snapshot(), "preempted task result must be dropped by the worker")
}

// TestGracefulDrainWaitsForInflight: Drain blocks until running tasks finish,
// while no new tasks are accepted.
func TestGracefulDrainWaitsForInflight(t *testing.T) {
	rep := &fakeReporter{}
	var release = make(chan struct{})
	var completed int32
	var mu sync.Mutex

	w := workerpool.New(workerpool.Options{
		ID: "w2", Name: "w2", Slots: 2,
		Handlers: workerpool.Handlers{
			"gate": func(ctx context.Context, _ *domain.Task) ([]byte, error) {
				<-release
				mu.Lock()
				completed++
				mu.Unlock()
				return []byte("ok"), nil
			},
		},
		Reporter:     rep,
		LeaseSeconds: 30,
	})

	t1 := &domain.Task{ID: "t1", Type: "gate", Priority: domain.PriorityNormal, Attempts: 1, TimeoutSeconds: 60, LeaseExpiresAt: ptr(time.Now().Add(30 * time.Second))}
	t2 := &domain.Task{ID: "t2", Type: "gate", Priority: domain.PriorityNormal, Attempts: 1, TimeoutSeconds: 60, LeaseExpiresAt: ptr(time.Now().Add(30 * time.Second))}
	require.True(t, w.ReserveSlot("t1"))
	require.True(t, w.ReserveSlot("t2"))
	require.NoError(t, w.Deliver(context.Background(), t1))
	require.NoError(t, w.Deliver(context.Background(), t2))

	// Begin draining: new reservations refused immediately.
	drainDone := make(chan struct{})
	go func() { w.Drain(context.Background()); close(drainDone) }()
	require.Eventually(t, func() bool { return !w.ReserveSlot("new") }, time.Second, 10*time.Millisecond)

	select {
	case <-drainDone:
		t.Fatal("Drain returned before in-flight tasks completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Drain did not return after tasks completed")
	}
}

// TestSuccessReportsResult: a normal successful task reports success.
func TestSuccessReportsResult(t *testing.T) {
	rep := &fakeReporter{}
	w := workerpool.New(workerpool.Options{
		ID: "w3", Name: "w3", Slots: 1,
		Handlers: workerpool.Handlers{
			"echo": func(_ context.Context, t *domain.Task) ([]byte, error) { return t.Payload, nil },
		},
		Reporter:     rep,
		LeaseSeconds: 30,
	})
	task := &domain.Task{ID: "t3", Type: "echo", Payload: []byte(`{"a":1}`), Attempts: 1, TimeoutSeconds: 5, LeaseExpiresAt: ptr(time.Now().Add(30 * time.Second))}
	require.True(t, w.ReserveSlot("t3"))
	require.NoError(t, w.Deliver(context.Background(), task))

	require.Eventually(t, func() bool { return len(rep.snapshot()) == 1 }, time.Second, 10*time.Millisecond)
	r := rep.snapshot()[0]
	require.True(t, r.success)
	require.Equal(t, "t3", r.taskID)
}

func ptr(t time.Time) *time.Time { return &t }
