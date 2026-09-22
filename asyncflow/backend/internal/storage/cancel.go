package storage

import (
	"context"
	"errors"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// CancelTask moves a non-terminal task to canceled. Running tasks are also
// canceled; their worker's completion is ignored by the ownership check.
func (s *Store) CancelTask(ctx context.Context, id string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM tasks WHERE id=$1`, id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if domain.TerminalStatuses[domain.TaskStatus(status)] {
			return ErrConflict
		}
		if _, err := tx.Exec(ctx,
			`UPDATE tasks SET status='canceled', updated_at=now(), finished_at=now() WHERE id=$1`, id); err != nil {
			return err
		}
		return writeAudit(ctx, tx, "task", id, "canceled", status, "canceled", "api", "manual cancellation")
	})
}
