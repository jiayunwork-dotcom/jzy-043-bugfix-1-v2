package storage

import (
	"context"
	"errors"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// PromotePending transitions non-terminal waiting tasks whose execute_after
// has passed back to ready (delay & retry waiting area -> ready queue).
// Returns the promoted tasks. The caller pushes them into Redis ready queues.
// limit caps one scan batch to keep latency bounded.
func (s *Store) PromotePending(ctx context.Context, now time.Time, limit int) ([]*domain.Task, error) {
	var out []*domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE tasks SET status='ready', updated_at=now()
			WHERE id IN (
				SELECT id FROM tasks
				WHERE status='pending'
				  AND (execute_after IS NULL OR execute_after <= $1)
				ORDER BY execute_after NULLS FIRST
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			)
			RETURNING `+taskColumns, now, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				return err
			}
			out = append(out, t)
		}
		return rows.Err()
	})
	return out, err
}

// MarkReadyByIDs transitions specific waiting tasks pending -> ready after
// they popped due from a Redis wait set. Only tasks still pending and whose
// execute_after has passed are moved; others are ignored (they will be
// re-added to Redis on the next reconciliation if still pending).
func (s *Store) MarkReadyByIDs(ctx context.Context, ids []string, now time.Time) ([]*domain.Task, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []*domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE tasks SET status='ready', updated_at=now()
			WHERE id = ANY($1) AND status='pending'
			  AND (execute_after IS NULL OR execute_after <= $2)
			RETURNING `+taskColumns, ids, now)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				return err
			}
			out = append(out, t)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		for _, t := range out {
			if err := writeAudit(ctx, tx, "task", t.ID, "promoted", "pending", "ready", "delay-scheduler",
				"delay/retry wait elapsed"); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// ListPending returns all non-terminal waiting tasks.
func (s *Store) ListPending(ctx context.Context) ([]PendingEntry, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, execute_after FROM tasks WHERE status='pending'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingEntry
	for rows.Next() {
		var e PendingEntry
		if err := rows.Scan(&e.ID, &e.ExecuteAfter); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// PendingEntry pairs a waiting task with its due time.
type PendingEntry struct {
	ID           string
	ExecuteAfter *time.Time
}

// RequeueReady moves tasks (e.g. reclaimed/preempted) explicitly to ready.
// It is idempotent: only pending/running/paused tasks are eligible depending
// on fromStatuses; the Redis push is done by the caller afterwards.
func (s *Store) RequeueReady(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE tasks SET status='ready', worker_id='', lease_expires_at=NULL,
				execute_after=NULL, updated_at=now()
			WHERE id = ANY($1) AND status <> 'ready'
			  AND status NOT IN ('succeeded','canceled')`, ids)
		return err
	})
}

// RecoverInflight reconciles Redis/Postgres after a (re)start: every
// non-terminal task not in a waiting state is normalized so that no work is
// lost or double-run across an engine crash.
//   - running tasks -> ready (their old worker process is gone on restart)
//   - ready tasks are returned for re-enqueueing
//   - pending tasks remain pending (delay/retry scans pick them back up)
func (s *Store) RecoverInflight(ctx context.Context) (readyIDs []*domain.Task, reclaimed int, err error) {
	err = s.withTx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status='ready', worker_id='', lease_expires_at=NULL, updated_at=now()
			WHERE status='running'`)
		if err != nil {
			return err
		}
		reclaimed = int(ct.RowsAffected())

		rows, err := tx.Query(ctx, `SELECT `+taskColumns+
			` FROM tasks WHERE status='ready' ORDER BY created_at`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				return err
			}
			readyIDs = append(readyIDs, t)
		}
		return rows.Err()
	})
	if reclaimed > 0 {
		_ = s.Audit(ctx, "task", "*", "startup_recovery", "running", "ready", "bootstrap",
			"reclaimed running tasks on engine start")
	}
	return readyIDs, reclaimed, err
}

// CountDead returns the current dead-letter backlog size.
func (s *Store) CountDead(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='dead'`).Scan(&n)
	return n, err
}

// DeadAggregateRow is one error-category bucket in the dead-letter view.
type DeadAggregateRow struct {
	Category string `json:"category"`
	Count    int    `json:"count"`
}

// ListDead returns dead-letter tasks with their attempts, newest first.
func (s *Store) ListDead(ctx context.Context, limit, offset int) ([]*domain.DeadLetter, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='dead'`).Scan(&total); err != nil {
		return nil, 0, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+taskColumns+
		` FROM tasks WHERE status='dead' ORDER BY updated_at DESC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	var tasks []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			rows.Close()
			return nil, 0, err
		}
		tasks = append(tasks, t)
	}
	rows.Close()

	out := make([]*domain.DeadLetter, 0, len(tasks))
	for _, t := range tasks {
		atts, err := s.Attempts(ctx, t.ID)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, &domain.DeadLetter{Task: *t, Attempts: atts, DeadAt: t.UpdatedAt})
	}
	return out, total, nil
}

// RetryDead re-enqueues a batch of dead tasks: dead -> ready.
// Returns the number of tasks actually moved and the moved tasks.
func (s *Store) RetryDead(ctx context.Context, ids []string) ([]*domain.Task, int, error) {
	var moved []*domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE tasks SET status='ready', last_error='', error_category='',
				finished_at=NULL, execute_after=NULL, updated_at=now()
			WHERE id = ANY($1) AND status='dead'
			RETURNING `+taskColumns, ids)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, err := scanTask(rows)
			if err != nil {
				return err
			}
			moved = append(moved, t)
			if err := writeAudit(ctx, tx, "task", t.ID, "dead_retry", "dead", "ready", "api",
				"dead letter manually re-enqueued"); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	return moved, len(moved), err
}

// DiscardDead marks dead tasks canceled (terminal; removed from DLQ view).
func (s *Store) DiscardDead(ctx context.Context, ids []string) (int, error) {
	n := 0
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE tasks SET status='canceled', updated_at=now()
			WHERE id = ANY($1) AND status='dead'`, ids)
		if err != nil {
			return err
		}
		n = int(ct.RowsAffected())
		for _, id := range ids {
			if err := writeAudit(ctx, tx, "task", id, "dead_discard", "dead", "canceled", "api", ""); err != nil {
				return err
			}
		}
		return nil
	})
	return n, err
}

// AggregateDead buckets dead tasks by error category.
func (s *Store) AggregateDead(ctx context.Context) ([]DeadAggregateRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(error_category,''), 'unknown'), count(*)
		FROM tasks WHERE status='dead' GROUP BY 1 ORDER BY 2 DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadAggregateRow
	for rows.Next() {
		var r DeadAggregateRow
		if err := rows.Scan(&r.Category, &r.Count); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// EnsureReady makes sure a single task is in ready status (used after
// Redis-based requeue for dead retries).
func (s *Store) EnsureReady(ctx context.Context, id string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+taskColumns+
		` FROM tasks WHERE id=$1 AND status='ready'`, id)
	t, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}
