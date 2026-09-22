package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// scanTask scans a task row in the column order used by taskColumns().
func scanTask(row pgx.Row) (*domain.Task, error) {
	var t domain.Task
	var payload []byte
	var retryKind, retryCron string
	var executeAfter, leaseExp, startedAt, finishedAt *time.Time
	var workerID, idemKey, callback, dagID, dagNode, lastError, errCat string
	err := row.Scan(
		&t.ID, &idemKey, &t.Type, &payload, &t.Priority, &t.Status,
		&t.MaxRetries, &t.Attempts, &t.TimeoutSeconds,
		&retryKind, &t.RetryPolicy.BaseInterval, &retryCron,
		&callback, &executeAfter, &workerID, &leaseExp,
		&dagID, &dagNode, &lastError, &errCat,
		&t.CreatedAt, &t.UpdatedAt, &startedAt, &finishedAt,
	)
	if err != nil {
		return nil, err
	}
	t.IDempotencyKey = idemKey
	t.Payload = payload
	t.RetryPolicy.Kind = domain.RetryKind(retryKind)
	t.RetryPolicy.CronExpression = retryCron
	t.RetryPolicy.MaxRetries = t.MaxRetries
	t.CallbackURL = callback
	t.ExecuteAfter = executeAfter
	t.WorkerID = workerID
	t.LeaseExpiresAt = leaseExp
	t.DAGID = dagID
	t.DAGNodeID = dagNode
	t.LastError = lastError
	t.ErrCategory = errCat
	t.StartedAt = startedAt
	t.FinishedAt = finishedAt
	return &t, nil
}

// taskColumns matches scanTask.
const taskColumns = `id, idempotency_key, type, payload, priority, status,
	max_retries, attempts, timeout_seconds,
	retry_kind, retry_base, retry_cron,
	callback_url, execute_after, worker_id, lease_expires_at,
	dag_id, dag_node_id, last_error, error_category,
	created_at, updated_at, started_at, finished_at`

func insertTaskSQL() string {
	return `INSERT INTO tasks (` + taskColumns + `) VALUES (
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		COALESCE($21, now()), COALESCE($22, now()), $23, $24)`
}

func taskArgs(t *domain.Task) []any {
	return []any{
		t.ID, t.IDempotencyKey, t.Type, t.Payload, string(t.Priority), t.Status,
		t.MaxRetries, t.Attempts, t.TimeoutSeconds,
		string(t.RetryPolicy.Kind), t.RetryPolicy.BaseInterval, t.RetryPolicy.CronExpression,
		t.CallbackURL, t.ExecuteAfter, t.WorkerID, t.LeaseExpiresAt,
		t.DAGID, t.DAGNodeID, t.LastError, t.ErrCategory,
		t.CreatedAt, t.UpdatedAt, t.StartedAt, t.FinishedAt,
	}
}

// CreateTask inserts a new task. Idempotency-key conflicts return ErrConflict.
func (s *Store) CreateTask(ctx context.Context, t *domain.Task) error {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = nowUTC()
	}
	t.UpdatedAt = t.CreatedAt
	if t.RetryPolicy.Kind == "" {
		t.RetryPolicy.Kind = domain.RetryExponential
	}
	if t.MaxRetries == 0 && t.RetryPolicy.MaxRetries > 0 {
		t.MaxRetries = t.RetryPolicy.MaxRetries
	}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, insertTaskSQL(), taskArgs(t)...)
		if err != nil {
			if strings.Contains(err.Error(), "tasks_idem_uniq") ||
				strings.Contains(err.Error(), "duplicate key") {
				return ErrConflict
			}
			return err
		}
		return writeAudit(ctx, tx, "task", t.ID, "created", "", string(t.Status), t.Type,
			fmt.Sprintf("priority=%s type=%s", t.Priority, t.Type))
	})
	return err
}

// GetTask fetches a task by ID.
func (s *Store) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id=$1`, id)
	t, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// GetTaskByIdem fetches by idempotency key; nil if absent.
func (s *Store) GetTaskByIdem(ctx context.Context, key string) (*domain.Task, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+taskColumns+
		` FROM tasks WHERE idempotency_key=$1`, key)
	t, err := scanTask(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return t, err
}

// TaskFilter narrows a list query.
type TaskFilter struct {
	Statuses []string
	Priority string
	Type     string
	DAGID    string
	From     *time.Time
	To       *time.Time
	Limit    int
	Offset   int
}

// ListTasks returns filtered tasks newest first plus total matching count.
func (s *Store) ListTasks(ctx context.Context, f TaskFilter) ([]*domain.Task, int, error) {
	var conds []string
	var args []any
	add := func(q string, v any) {
		args = append(args, v)
		conds = append(conds, fmt.Sprintf(q, len(args)))
	}
	if len(f.Statuses) > 0 {
		conds = append(conds, fmt.Sprintf("status = ANY($%d)", len(args)+1))
		args = append(args, f.Statuses)
	}
	if f.Priority != "" {
		add("priority = $%d", f.Priority)
	}
	if f.Type != "" {
		add("type = $%d", f.Type)
	}
	if f.DAGID != "" {
		add("dag_id = $%d", f.DAGID)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at <= $%d", *f.To)
	}
	where := ""
	if len(conds) > 0 {
		where = "WHERE " + strings.Join(conds, " AND ")
	}
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args = append(args, limit, f.Offset)
	q := `SELECT ` + taskColumns + ` FROM tasks ` + where +
		fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, len(args)-1, len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, t)
	}
	return out, total, rows.Err()
}

// TaskStatusCount groups counts by status.
func (s *Store) TaskStatusCount(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM tasks GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
