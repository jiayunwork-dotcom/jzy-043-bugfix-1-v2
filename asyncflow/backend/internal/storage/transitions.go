package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// BeginAttempt atomically leases a queued task to a worker:
// status ready -> running, attempts bumped, an attempt row inserted.
// Returns ErrConflict if the task is no longer queued (already taken,
// preempted away, dead, etc.), which is the core guard against duplicate
// execution.
func (s *Store) BeginAttempt(ctx context.Context, taskID, workerID string, lease time.Time) (*domain.Task, error) {
	var out *domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status='running', worker_id=$2, lease_expires_at=$3,
			    attempts=attempts+1,
			    started_at=COALESCE(started_at, now()), updated_at=now()
			WHERE id=$1 AND status='ready'
			RETURNING `+taskColumns, taskID, workerID, lease)
		t, err := scanTask(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO task_attempts(task_id, attempt_no, worker_id, started_at, status)
			VALUES ($1,$2,$3, now(), 'running')`,
			t.ID, t.Attempts, workerID); err != nil {
			return err
		}
		if err := writeAudit(ctx, tx, "task", t.ID, "leased", "ready", "running", workerID,
			fmt.Sprintf("attempt=%d", t.Attempts)); err != nil {
			return err
		}
		out = t
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RenewLease extends a running task's lease. Fails (false) if the task is no
// longer owned by that worker (preempted or reclaimed).
func (s *Store) RenewLease(ctx context.Context, taskID, workerID string, expiry time.Time) (bool, error) {
	ct, err := s.pool.Exec(ctx, `
		UPDATE tasks SET lease_expires_at=$3
		WHERE id=$1 AND worker_id=$2 AND status='running'`,
		taskID, workerID, expiry)
	if err != nil {
		return false, err
	}
	return ct.RowsAffected() == 1, nil
}

// CompletionOutcome tells what happened after an attempt was finalized.
type CompletionOutcome string

const (
	OutcomeSucceeded CompletionOutcome = "succeeded"
	OutcomeRetrying  CompletionOutcome = "retrying"
	OutcomeDead      CompletionOutcome = "dead"
)

// CompleteResult is the input for finalizing one attempt.
type CompleteResult struct {
	TaskID   string
	WorkerID string
	Success  bool
	Result   []byte
	Error    string
	Category string
	// RetryAfter is when the next attempt may run (zero = no retry).
	RetryAfter time.Time
}

// CompleteAttempt records the attempt result and performs the next state
// transition atomically. Returns the updated task and outcome.
// ErrConflict indicates the worker had lost ownership; the result is ignored.
func (s *Store) CompleteAttempt(ctx context.Context, in CompleteResult) (*domain.Task, CompletionOutcome, error) {
	var out *domain.Task
	var outcome CompletionOutcome
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `SELECT `+taskColumns+
			` FROM tasks WHERE id=$1 FOR UPDATE`, in.TaskID)
		t, err := scanTask(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if t.Status != domain.StatusRunning || t.WorkerID != in.WorkerID {
			return ErrConflict
		}

		attemptStatus := "failed"
		if in.Success {
			attemptStatus = "succeeded"
		}
		if _, err := tx.Exec(ctx, `
			UPDATE task_attempts
			SET ended_at=now(), status=$3, error=$4, error_category=$5, result=$6
			WHERE task_id=$1 AND attempt_no=$2`,
			t.ID, t.Attempts, attemptStatus, in.Error, in.Category, nullableBytes(in.Result)); err != nil {
			return err
		}

		switch {
		case in.Success:
			outcome = OutcomeSucceeded
			if err := applyTaskUpdate(ctx, tx, t, taskUpdate{
				status:     st(domain.StatusSucceeded),
				finishedAt: ptrTime(nowUTC()),
				clearLease: true,
			}); err != nil {
				return err
			}
			if err := writeAudit(ctx, tx, "task", t.ID, "succeeded", "running", "succeeded", in.WorkerID, ""); err != nil {
				return err
			}
		case in.RetryAfter.IsZero():
			outcome = OutcomeDead
			if err := applyTaskUpdate(ctx, tx, t, taskUpdate{
				status:        st(domain.StatusDead),
				finishedAt:    ptrTime(nowUTC()),
				lastError:     ptrStr(in.Error),
				errorCategory: ptrStr(in.Category),
				clearLease:    true,
			}); err != nil {
				return err
			}
			if err := writeAudit(ctx, tx, "task", t.ID, "dead_letter", "running", "dead", in.WorkerID,
				fmt.Sprintf("category=%s attempts=%d", in.Category, t.Attempts)); err != nil {
				return err
			}
		default:
			outcome = OutcomeRetrying
			if err := applyTaskUpdate(ctx, tx, t, taskUpdate{
				status:        st(domain.StatusPending),
				executeAfter:  ptrTime(in.RetryAfter),
				lastError:     ptrStr(in.Error),
				errorCategory: ptrStr(in.Category),
				clearLease:    true,
			}); err != nil {
				return err
			}
			if err := writeAudit(ctx, tx, "task", t.ID, "retry_scheduled", "running", "pending", in.WorkerID,
				fmt.Sprintf("after=%s category=%s", in.RetryAfter.Format(time.RFC3339), in.Category)); err != nil {
				return err
			}
		}

		out, err = scanTask(tx.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id=$1`, t.ID))
		return err
	})
	if err != nil {
		return nil, "", err
	}
	return out, outcome, nil
}

// Preempt interrupts a running task for a higher-priority one. The task moves
// straight back to ready state (it is pushed to the queue head by the caller).
// Its in-flight attempt is recorded as interrupted. ErrConflict if the task
// stopped running between selection and preemption.
func (s *Store) Preempt(ctx context.Context, taskID, byWorker string) (*domain.Task, error) {
	var out *domain.Task
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tasks
			SET status='ready', worker_id='', lease_expires_at=NULL, updated_at=now()
			WHERE id=$1 AND status='running'
			RETURNING `+taskColumns, taskID)
		t, err := scanTask(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE task_attempts
			SET ended_at=now(), status='interrupted', error='preempted by critical task',
			    error_category='preempted'
			WHERE task_id=$1 AND ended_at IS NULL`, taskID); err != nil {
			return err
		}
		if err := writeAudit(ctx, tx, "task", taskID, "preempted", "running", "ready", byWorker,
			"interrupted for critical dispatch; back to queue head"); err != nil {
			return err
		}
		out = t
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReclaimedTask pairs a requeued task with the worker that lost it.
type ReclaimedTask struct {
	Task      *domain.Task
	OldWorker string
	Reason    string // lease_expired | worker_offline
}

// ReclaimExpired resets every running task whose lease has expired or whose
// owner is offline back to ready (queue push is the caller's job; startup
// reconciliation covers crashes).
func (s *Store) ReclaimExpired(ctx context.Context, now time.Time) ([]ReclaimedTask, error) {
	var out []ReclaimedTask
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+prefixedColumns("t")+`,
				CASE WHEN w.id IS NULL OR w.status='offline'
					THEN 'worker_offline' ELSE 'lease_expired' END AS reason
			FROM tasks t
			LEFT JOIN workers w ON w.id = t.worker_id
			WHERE t.status='running'
			  AND (t.lease_expires_at < $1 OR w.id IS NULL OR w.status='offline')
			FOR UPDATE OF t`, now)
		if err != nil {
			return err
		}
		type pair struct {
			t      *domain.Task
			reason string
		}
		var victims []pair
		for rows.Next() {
			targets, build := taskScanTargets()
			var reason string
			if err := rows.Scan(append(targets, &reason)...); err != nil {
				rows.Close()
				return err
			}
			victims = append(victims, pair{build(), reason})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, v := range victims {
			oldWorker := v.t.WorkerID
			if _, err := tx.Exec(ctx, `
				UPDATE tasks SET status='ready', worker_id='', lease_expires_at=NULL, updated_at=now()
				WHERE id=$1`, v.t.ID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE task_attempts
				SET ended_at=now(),
				    status=CASE WHEN ended_at IS NULL THEN 'timeout' ELSE status END,
				    error=CASE WHEN ended_at IS NULL THEN 'lease expired / worker lost' ELSE error END,
				    error_category=CASE WHEN ended_at IS NULL THEN 'timeout' ELSE error_category END
				WHERE task_id=$1`, v.t.ID); err != nil {
				return err
			}
			if err := writeAudit(ctx, tx, "task", v.t.ID, "reclaimed", "running", "ready", "reaper",
				"reason="+v.reason+" old_worker="+oldWorker); err != nil {
				return err
			}
			out = append(out, ReclaimedTask{Task: v.t, OldWorker: oldWorker, Reason: v.reason})
		}
		return nil
	})
	return out, err
}

func nullableBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
