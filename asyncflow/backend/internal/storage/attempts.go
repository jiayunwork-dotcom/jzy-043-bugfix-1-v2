package storage

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// Attempts returns all attempts of a task ordered by attempt number.
func (s *Store) Attempts(ctx context.Context, taskID string) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, task_id, attempt_no, worker_id, started_at, ended_at,
		       status, error, error_category, result
		FROM task_attempts WHERE task_id=$1 ORDER BY attempt_no`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Attempt
	for rows.Next() {
		var a domain.Attempt
		var endedAt *time.Time
		var result []byte
		var errStr, cat, worker string
		if err := rows.Scan(&a.ID, &a.TaskID, &a.AttemptNo, &worker,
			&a.StartedAt, &endedAt, &a.Status, &errStr, &cat, &result); err != nil {
			return nil, err
		}
		a.WorkerID = worker
		a.EndedAt = endedAt
		a.Error = errStr
		a.ErrCategory = cat
		a.Result = result
		out = append(out, a)
	}
	return out, rows.Err()
}
