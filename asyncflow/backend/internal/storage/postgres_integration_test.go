package storage_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/stretchr/testify/require"
)

// openTestDB connects to a real Postgres when TEST_DATABASE_DSN is set; the
// whole suite is skipped otherwise (unit tests use the in-memory fake).
func openTestDB(t *testing.T) *storage.Store {
	dsn := os.Getenv("TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("TEST_DATABASE_DSN not set; skipping Postgres integration tests")
	}
	ctx := context.Background()
	s, err := storage.New(ctx, dsn)
	require.NoError(t, err)
	require.NoError(t, s.Migrate(ctx, "../../migrations"))
	return s
}

func TestPostgresLifecycleCAS(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	id := "pg-it-" + time.Now().Format("150405.000000")

	tk := &domain.Task{
		ID: id, Type: "sample.echo", Payload: []byte(`{"x":1}`),
		Priority: domain.PriorityHigh, Status: domain.StatusReady,
		MaxRetries: 1, TimeoutSeconds: 10,
		RetryPolicy: domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 1, MaxRetries: 1},
	}
	require.NoError(t, s.CreateTask(ctx, tk))

	// First lease wins.
	leased, err := s.BeginAttempt(ctx, id, "w-a", time.Now().Add(30*time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, leased.Attempts)

	// Duplicate lease conflicts (no double execution).
	_, err = s.BeginAttempt(ctx, id, "w-b", time.Now().Add(30*time.Second))
	require.ErrorIs(t, err, storage.ErrConflict)

	// Failure with one retry allowed -> pending retry.
	after := time.Now().Add(time.Second)
	tk2, outcome, err := s.CompleteAttempt(ctx, storage.CompleteResult{
		TaskID: id, WorkerID: "w-a", Error: "boom", Category: "handler_error", RetryAfter: after,
	})
	require.NoError(t, err)
	require.Equal(t, storage.OutcomeRetrying, outcome)
	require.Equal(t, domain.StatusPending, tk2.Status)

	// Promote then fail again (retries exhausted) -> dead.
	promoted, err := s.MarkReadyByIDs(ctx, []string{id}, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Len(t, promoted, 1)
	leased2, err := s.BeginAttempt(ctx, id, "w-a", time.Now().Add(30*time.Second))
	require.NoError(t, err)
	require.Equal(t, 2, leased2.Attempts)
	_, outcome, err = s.CompleteAttempt(ctx, storage.CompleteResult{
		TaskID: id, WorkerID: "w-a", Error: "boom2", Category: "handler_error",
	})
	require.NoError(t, err)
	require.Equal(t, storage.OutcomeDead, outcome)

	n, err := s.CountDead(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)

	// Batch retry from dead letter -> ready.
	moved, cnt, err := s.RetryDead(ctx, []string{id})
	require.NoError(t, err)
	require.Equal(t, 1, cnt)
	require.Equal(t, domain.StatusReady, moved[0].Status)

	// Timeline shows both failed attempts.
	attempts, err := s.Attempts(ctx, id)
	require.NoError(t, err)
	require.Len(t, attempts, 2)
}
