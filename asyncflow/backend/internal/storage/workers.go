package storage

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// UpsertWorker registers a worker or refreshes its descriptor/heartbeat.
// Workers already marked offline are resurrected on re-register.
func (s *Store) UpsertWorker(ctx context.Context, w *domain.Worker) error {
	if w.CreatedAt.IsZero() {
		w.CreatedAt = nowUTC()
	}
	if w.LastHeartbeat.IsZero() {
		w.LastHeartbeat = nowUTC()
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO workers(id, name, capabilities, total_slots, used_slots,
			                    status, last_heartbeat, current_tasks,
			                    completed_count, failed_count, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (id) DO UPDATE SET
				name=EXCLUDED.name,
				capabilities=EXCLUDED.capabilities,
				total_slots=EXCLUDED.total_slots,
				status=CASE WHEN workers.status='offline'
				            THEN 'online' ELSE EXCLUDED.status END,
				last_heartbeat=GREATEST(workers.last_heartbeat, EXCLUDED.last_heartbeat)
		`, w.ID, w.Name, w.Capabilities, w.TotalSlots, w.UsedSlots,
			w.Status, w.LastHeartbeat, w.CurrentTaskIDs,
			w.Completed, w.Failed, w.CreatedAt)
		return err
	})
}

// Heartbeat updates liveness, slot usage and current task list atomically.
func (s *Store) Heartbeat(ctx context.Context, w *domain.Worker) error {
	w.LastHeartbeat = nowUTC()
	ct, err := s.pool.Exec(ctx, `
		UPDATE workers
		SET last_heartbeat=$2, used_slots=$3, current_tasks=$4, status='online'
		WHERE id=$1`,
		w.ID, w.LastHeartbeat, w.UsedSlots, w.CurrentTaskIDs)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return s.UpsertWorker(ctx, w)
	}
	return nil
}

// MarkWorkerDraining flips status to draining (graceful shutdown requested).
func (s *Store) MarkWorkerDraining(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET status='draining' WHERE id=$1`, id)
	return err
}

// MarkWorkersOffline marks every worker whose last heartbeat is older than
// cutoff as offline. Returns IDs that flipped.
func (s *Store) MarkWorkersOffline(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE workers SET status='offline'
		WHERE status <> 'offline' AND last_heartbeat < $1
		RETURNING id`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// DeleteWorker removes a worker that finished graceful shutdown.
func (s *Store) DeleteWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE id=$1`, id)
	return err
}

// GetWorker fetches one worker.
func (s *Store) GetWorker(ctx context.Context, id string) (*domain.Worker, error) {
	row := s.pool.QueryRow(ctx, workerColumns+` FROM workers WHERE id=$1`, id)
	w, err := scanWorker(row)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// ListWorkers returns all workers including offline ones.
func (s *Store) ListWorkers(ctx context.Context) ([]*domain.Worker, error) {
	rows, err := s.pool.Query(ctx, workerColumns+
		` FROM workers ORDER BY status, last_heartbeat DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// IncWorkerStats bumps completed/failed counters after an attempt finishes.
func (s *Store) IncWorkerStats(ctx context.Context, id string, completed, failed bool) error {
	q := `UPDATE workers SET
		completed_count = completed_count + $2,
		failed_count = failed_count + $3
		WHERE id=$1`
	var c, f int
	if completed {
		c = 1
	}
	if failed {
		f = 1
	}
	_, err := s.pool.Exec(ctx, q, id, c, f)
	return err
}

const workerColumns = `id, name, capabilities, total_slots, used_slots,
	status, last_heartbeat, current_tasks, completed_count, failed_count, created_at`

func scanWorker(row interface {
	Scan(dest ...any) error
}) (*domain.Worker, error) {
	var w domain.Worker
	err := row.Scan(&w.ID, &w.Name, &w.Capabilities, &w.TotalSlots, &w.UsedSlots,
		&w.Status, &w.LastHeartbeat, &w.CurrentTaskIDs, &w.Completed, &w.Failed, &w.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &w, nil
}
