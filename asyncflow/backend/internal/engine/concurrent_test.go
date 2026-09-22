package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

// TestConcurrentSubmitNoLossNoDuplicate: under heavy concurrent submission
// and claiming, every task is either ready or leased exactly once — nothing
// is lost or executed twice.
func TestConcurrentSubmitNoLossNoDuplicate(t *testing.T) {
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	ctx := context.Background()
	eng := engine.New(fs, mq, nil, nil)

	const submitters = 16
	const perSubmitter = 50
	const total = submitters * perSubmitter

	var wg sync.WaitGroup
	for s := 0; s < submitters; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			for i := 0; i < perSubmitter; i++ {
				_, _, err := eng.Submit(ctx, engine.SubmitInput{
					Type: "sample.echo", Priority: domain.PriorityNormal,
					TimeoutSeconds: 10,
					RetryPolicy:    domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 1},
				})
				if err != nil {
					t.Errorf("submit error: %v", err)
					return
				}
			}
		}(s)
	}
	wg.Wait()

	// Exactly `total` tasks exist and all are ready.
	tasks, count, err := fs.ListAllTasks()
	require.NoError(t, err)
	require.Equal(t, total, len(tasks))
	require.Equal(t, total, count)
	ids := map[string]bool{}
	for _, tk := range tasks {
		require.Equal(t, domain.StatusReady, tk.Status)
		require.False(t, ids[tk.ID], "duplicate task id")
		ids[tk.ID] = true
	}

	// Concurrent claimers drain the queue; every task id must be leased by
	// exactly one worker (CAS serializes BeginAttempt).
	claimers := 8
	var claimMu sync.Mutex
	claimed := map[string]string{}
	var cwg sync.WaitGroup
	stop := make(chan struct{})

	for c := 0; c < claimers; c++ {
		cwg.Add(1)
		go func(worker string) {
			defer cwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id, err := mq.Dequeue(ctx, domain.PriorityNormal)
				if err != nil || id == "" {
					return
				}
				_, err = fs.BeginAttempt(ctx, id, worker, time.Now().Add(30*time.Second))
				if err != nil {
					// Lost the race: someone else owns it; must not be lost.
					continue
				}
				claimMu.Lock()
				_, dup := claimed[id]
				require.False(t, dup, "task %s executed twice", id)
				claimed[id] = worker
				claimMu.Unlock()
			}
		}("w" + string(rune('0'+c)))
	}
	cwg.Wait()
	close(stop)

	require.Len(t, claimed, total, "every task claimed exactly once")
}
