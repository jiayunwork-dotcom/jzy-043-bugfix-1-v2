package storage

import (
	"context"
	"strings"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// prefixedColumns returns the task column list qualified by an alias.
func prefixedColumns(alias string) string {
	cols := strings.Split(taskColumns, ", ")
	for i, c := range cols {
		cols[i] = alias + "." + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

// taskScanTargets returns scan targets for the 24 task columns and a function
// to build the populated Task after the scan completes.
func taskScanTargets() (targets []any, build func() *domain.Task) {
	var t domain.Task
	var payload []byte
	var retryKind, retryCron string
	var executeAfter, leaseExp, startedAt, finishedAt *time.Time
	var workerID, idemKey, callback, dagID, dagNode, lastError, errCat string
	targets = []any{
		&t.ID, &idemKey, &t.Type, &payload, &t.Priority, &t.Status,
		&t.MaxRetries, &t.Attempts, &t.TimeoutSeconds,
		&retryKind, &t.RetryPolicy.BaseInterval, &retryCron,
		&callback, &executeAfter, &workerID, &leaseExp,
		&dagID, &dagNode, &lastError, &errCat,
		&t.CreatedAt, &t.UpdatedAt, &startedAt, &finishedAt,
	}
	build = func() *domain.Task {
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
		return &t
	}
	return targets, build
}

// scanTaskPrefixed scans a row whose first 24 columns are a prefixed task.
func scanTaskPrefixed(rows pgx.Rows) (*domain.Task, error) {
	targets, build := taskScanTargets()
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	return build(), nil
}

// taskUpdate is a partial task update applied within a transaction.
type taskUpdate struct {
	status        *string
	executeAfter  *time.Time
	finishedAt    *time.Time
	lastError     *string
	errorCategory *string
	clearLease    bool
}

func st(s domain.TaskStatus) *string { v := string(s); return &v }
func ptrTime(t time.Time) *time.Time { return &t }
func ptrStr(s string) *string        { return &s }

func applyTaskUpdate(ctx context.Context, tx pgx.Tx, t *domain.Task, u taskUpdate) error {
	sets := []string{"updated_at=now()"}
	args := []any{}
	add := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, col+"=$"+itoa(len(args)))
	}
	if u.status != nil {
		add("status", *u.status)
		t.Status = domain.TaskStatus(*u.status)
	}
	if u.executeAfter != nil {
		add("execute_after", *u.executeAfter)
		t.ExecuteAfter = u.executeAfter
	}
	if u.finishedAt != nil {
		add("finished_at", *u.finishedAt)
		t.FinishedAt = u.finishedAt
	}
	if u.lastError != nil {
		add("last_error", *u.lastError)
		t.LastError = *u.lastError
	}
	if u.errorCategory != nil {
		add("error_category", *u.errorCategory)
		t.ErrCategory = *u.errorCategory
	}
	if u.clearLease {
		sets = append(sets, "worker_id=''", "lease_expires_at=NULL")
		t.WorkerID = ""
		t.LeaseExpiresAt = nil
	}
	args = append(args, t.ID)
	_, err := tx.Exec(ctx,
		"UPDATE tasks SET "+strings.Join(sets, ", ")+" WHERE id=$"+itoa(len(args)),
		args...)
	return err
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
