// Package engine wires task submission, result reporting and the three retry
// strategies (exponential backoff, fixed interval, cron) with the dead-letter
// terminal state.
package engine

import (
	"context"
	"errors"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/errcat"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/storage"
)

var (
	ErrInvalidPriority = errors.New("invalid priority")
	ErrInvalidType     = errors.New("task type is required")
)

// Storer is the persistence surface used by the engine.
type Storer interface {
	CreateTask(ctx context.Context, t *domain.Task) error
	GetTaskByIdem(ctx context.Context, key string) (*domain.Task, error)
	CompleteAttempt(ctx context.Context, in storage.CompleteResult) (*domain.Task, storage.CompletionOutcome, error)
	RenewLease(ctx context.Context, taskID, workerID string, expiry time.Time) (bool, error)
}

// Queuer is the Redis data plane.
type Queuer interface {
	Enqueue(ctx context.Context, p domain.Priority, taskID string) error
	EnqueueHead(ctx context.Context, p domain.Priority, taskID string) error
	AddWaiting(ctx context.Context, kind queue.WaitKind, taskID string, dueAt time.Time) error
	RemoveWaiting(ctx context.Context, kind queue.WaitKind, taskID string) error
}

// DAGHook reacts to terminal task events; nil for standalone tasks.
type DAGHook interface {
	OnTaskTerminal(ctx context.Context, t *domain.Task, outcome storage.CompletionOutcome, errMsg string)
}

// CallbackPoster posts completion callbacks (nil-safe wrapper in callback.go).
type CallbackPoster interface {
	Post(ctx context.Context, url string, t *domain.Task, success bool, errMsg string)
}

// Engine coordinates submissions and completions.
type Engine struct {
	store Storer
	q     Queuer
	dag   DAGHook
	cb    CallbackPoster
}

func New(store Storer, q Queuer, dag DAGHook, cb CallbackPoster) *Engine {
	return &Engine{store: store, q: q, dag: dag, cb: cb}
}

// SetDAGHook installs the DAG terminal-event hook (used to break the
// engine <-> orchestration wiring cycle).
func (e *Engine) SetDAGHook(h DAGHook) { e.dag = h }

// SubmitInput is an inbound task request.
type SubmitInput struct {
	Type           string
	Payload        []byte
	Priority       domain.Priority
	IDempotencyKey string
	MaxRetries     int
	TimeoutSeconds int
	RetryPolicy    domain.RetryPolicy
	CallbackURL    string
	ExecuteAfter   *time.Time
}

// Submit persists a task and places it in either the ready queue or the delay
// waiting area. Idempotency-key duplicates return (existing, true, nil).
func (e *Engine) Submit(ctx context.Context, in SubmitInput) (*domain.Task, bool, error) {
	if in.Priority == "" {
		in.Priority = domain.PriorityNormal
	}
	if !in.Priority.Valid() {
		return nil, false, ErrInvalidPriority
	}
	if in.Type == "" {
		return nil, false, ErrInvalidType
	}
	if in.TimeoutSeconds <= 0 {
		in.TimeoutSeconds = 60
	}
	if in.RetryPolicy.Kind == "" {
		in.RetryPolicy.Kind = domain.RetryExponential
	}
	if in.MaxRetries == 0 {
		in.MaxRetries = in.RetryPolicy.MaxRetries
	}
	if in.RetryPolicy.MaxRetries == 0 {
		in.RetryPolicy.MaxRetries = in.MaxRetries
	}
	if in.RetryPolicy.BaseInterval == 0 {
		in.RetryPolicy.BaseInterval = 5
	}

	if in.IDempotencyKey != "" {
		if existing, err := e.store.GetTaskByIdem(ctx, in.IDempotencyKey); err != nil {
			return nil, false, err
		} else if existing != nil {
			return existing, true, nil
		}
	}

	t := &domain.Task{
		ID:             newID(),
		IDempotencyKey: in.IDempotencyKey,
		Type:           in.Type,
		Payload:        in.Payload,
		Priority:       in.Priority,
		MaxRetries:     in.MaxRetries,
		TimeoutSeconds: in.TimeoutSeconds,
		RetryPolicy:    in.RetryPolicy,
		CallbackURL:    in.CallbackURL,
		ExecuteAfter:   in.ExecuteAfter,
	}
	now := time.Now().UTC()
	delayed := in.ExecuteAfter != nil && in.ExecuteAfter.After(now)
	if delayed {
		t.Status = domain.StatusPending
	} else {
		t.Status = domain.StatusReady
		t.ExecuteAfter = nil
	}
	if err := e.store.CreateTask(ctx, t); err != nil {
		return nil, false, err
	}
	if delayed {
		if err := e.q.AddWaiting(ctx, queue.WaitDelay, t.ID, *in.ExecuteAfter); err != nil {
			return nil, false, err
		}
	} else {
		if err := e.q.Enqueue(ctx, t.Priority, t.ID); err != nil {
			return nil, false, err
		}
	}
	return t, false, nil
}

// ReportResult finalizes an attempt, schedules a retry or moves the task to
// the dead-letter area, and fires callbacks / DAG hooks.
func (e *Engine) ReportResult(ctx context.Context, t *domain.Task, success bool, result []byte, errMsg, category string) {
	in := storage.CompleteResult{
		TaskID:   t.ID,
		WorkerID: t.WorkerID,
		Success:  success,
		Result:   result,
		Error:    errMsg,
		Category: category,
	}
	if !success {
		if category == "" {
			category = string(errcat.Classify(errMsg))
		}
		in.Category = category
		// Attempts reflects the run that just finished. A retry remains if the
		// failed attempt number is within max retries.
		if t.Attempts <= t.MaxRetries {
			if due, err := t.RetryPolicy.NextDelay(t.Attempts, time.Now().UTC()); err == nil {
				in.RetryAfter = time.Now().UTC().Add(time.Duration(due) * time.Second)
			}
		}
	}

	updated, outcome, err := e.store.CompleteAttempt(ctx, in)
	if err != nil {
		// Lost the ownership race (preempted/reclaimed): nothing to schedule.
		return
	}
	switch outcome {
	case storage.OutcomeSucceeded:
		if e.cb != nil {
			e.cb.Post(ctx, updated.CallbackURL, updated, true, "")
		}
	case storage.OutcomeDead:
		if e.cb != nil {
			e.cb.Post(ctx, updated.CallbackURL, updated, false, errMsg)
		}
	case storage.OutcomeRetrying:
		if updated.ExecuteAfter != nil {
			_ = e.q.AddWaiting(ctx, queue.WaitRetry, updated.ID, *updated.ExecuteAfter)
		}
	}
	if e.dag != nil && (outcome == storage.OutcomeSucceeded || outcome == storage.OutcomeDead) {
		e.dag.OnTaskTerminal(ctx, updated, outcome, errMsg)
	}
}

// RenewLease extends a running task's lease; false when ownership was lost.
func (e *Engine) RenewLease(ctx context.Context, taskID, workerID string, lease time.Duration) bool {
	ok, err := e.store.RenewLease(ctx, taskID, workerID, time.Now().UTC().Add(lease))
	return err == nil && ok
}
