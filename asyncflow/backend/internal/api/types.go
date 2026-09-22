// Package api implements the HTTP API consumed by the management panel and by
// external workers.
package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/asyncflow/engine/internal/delayscheduler"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/runtime"
	"github.com/asyncflow/engine/internal/storage"
)

// TaskStorer is the Postgres surface the API reads/mutates tasks through.
type TaskStorer interface {
	CreateTask(ctx context.Context, t *domain.Task) error
	GetTask(ctx context.Context, id string) (*domain.Task, error)
	ListTasks(ctx context.Context, f storage.TaskFilter) ([]*domain.Task, int, error)
	Attempts(ctx context.Context, taskID string) ([]domain.Attempt, error)
	CancelTask(ctx context.Context, id string) error
	ListDead(ctx context.Context, limit, offset int) ([]*domain.DeadLetter, int, error)
	AggregateDead(ctx context.Context) ([]storage.DeadAggregateRow, error)
	RetryDead(ctx context.Context, ids []string) ([]*domain.Task, int, error)
	DiscardDead(ctx context.Context, ids []string) (int, error)
	ListWorkers(ctx context.Context) ([]*domain.Worker, error)
	GetWorker(ctx context.Context, id string) (*domain.Worker, error)
	MarkWorkerDraining(ctx context.Context, id string) error
	IncWorkerStats(ctx context.Context, id string, completed, failed bool) error
	Heartbeat(ctx context.Context, w *domain.Worker) error
	UpsertWorker(ctx context.Context, w *domain.Worker) error
	GetDAG(ctx context.Context, id string) (*domain.DAG, error)
	ListDAGs(ctx context.Context, limit int) ([]*domain.DAG, error)
	ListDAGNodes(ctx context.Context, dagID string) ([]*domain.DAGNode, error)
	ListAudit(ctx context.Context, entity, entityID string, limit int) ([]map[string]any, error)
}

// QueueSink is the Redis surface used after API-triggered state changes.
type QueueSink interface {
	Enqueue(ctx context.Context, p domain.Priority, taskID string) error
	RemoveWaitingKind(ctx context.Context, kind, taskID string) error
}

// ExternalRegistrar covers the external worker HTTP protocol.
type ExternalRegistrar interface {
	Register(ctx context.Context, id, name string, caps []string, slots int) (runtime.ClaimHandle, error)
	Heartbeat(ctx context.Context, id string, taskIDs []string) error
	Get(id string) runtime.ClaimHandle
	TaskStarted(workerID string, t *domain.Task)
	TaskFinished(workerID, taskID string)
	SnapshotIDs(workerID string) []string
}

// Deps bundles everything the handlers need.
type Deps struct {
	Store   TaskStorer
	Engine  *engine.Engine
	DAG     *orchestration.Engine
	Metrics *metrics.Collector
	ExtMgr  ExternalRegistrar
	Queue   QueueSink
	Delay   *delayscheduler.Runner
}

// Server holds request-scoped dependencies.
type Server struct{ d Deps }

func NewServer(d Deps) *Server { return &Server{d: d} }

// submitTaskRequest is the POST /api/tasks body.
type submitTaskRequest struct {
	Type           string          `json:"type"`
	Payload        any             `json:"payload"`
	Priority       domain.Priority `json:"priority"`
	IdempotencyKey string          `json:"idempotency_key"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	DelaySeconds   int             `json:"delay_seconds"`
	CallbackURL    string          `json:"callback_url"`
	RetryPolicy    *retryPolicyReq `json:"retry_policy"`
}

type retryPolicyReq struct {
	Kind           domain.RetryKind `json:"kind"`
	BaseInterval   int              `json:"base_interval_seconds"`
	MaxRetries     int              `json:"max_retries"`
	CronExpression string           `json:"cron_expression"`
}

func (r submitTaskRequest) toInput() (engine.SubmitInput, error) {
	payload, err := jsonMarshal(r.Payload)
	if err != nil {
		return engine.SubmitInput{}, err
	}
	in := engine.SubmitInput{
		Type:           r.Type,
		Payload:        payload,
		Priority:       r.Priority,
		IDempotencyKey: r.IdempotencyKey,
		MaxRetries:     r.MaxRetries,
		TimeoutSeconds: r.TimeoutSeconds,
		CallbackURL:    r.CallbackURL,
	}
	if r.RetryPolicy != nil {
		in.RetryPolicy = domain.RetryPolicy{
			Kind:           r.RetryPolicy.Kind,
			BaseInterval:   r.RetryPolicy.BaseInterval,
			MaxRetries:     r.RetryPolicy.MaxRetries,
			CronExpression: r.RetryPolicy.CronExpression,
		}
	}
	if r.DelaySeconds > 0 {
		after := time.Now().UTC().Add(time.Duration(r.DelaySeconds) * time.Second)
		in.ExecuteAfter = &after
	}
	return in, nil
}

// taskView is the JSON projection returned to the panel.
type taskView struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Priority       string          `json:"priority"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxRetries     int             `json:"max_retries"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	Payload        json.RawMessage `json:"payload"`
	CallbackURL    string          `json:"callback_url,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	ErrorCategory  string          `json:"error_category,omitempty"`
	DAGID          string          `json:"dag_id,omitempty"`
	DAGNodeID      string          `json:"dag_node_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

func toTaskView(t *domain.Task) taskView {
	payload := t.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	return taskView{
		ID: t.ID, Type: t.Type, Priority: string(t.Priority), Status: string(t.Status),
		Attempts: t.Attempts, MaxRetries: t.MaxRetries, TimeoutSeconds: t.TimeoutSeconds,
		Payload: payload, CallbackURL: t.CallbackURL, WorkerID: t.WorkerID,
		LastError: t.LastError, ErrorCategory: t.ErrCategory,
		DAGID: t.DAGID, DAGNodeID: t.DAGNodeID,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt,
		StartedAt: t.StartedAt, FinishedAt: t.FinishedAt,
	}
}

type attemptView struct {
	AttemptNo     int        `json:"attempt_no"`
	WorkerID      string     `json:"worker_id"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	Status        string     `json:"status"`
	Error         string     `json:"error,omitempty"`
	ErrorCategory string     `json:"error_category,omitempty"`
}

func toAttemptViews(as []domain.Attempt) []attemptView {
	out := make([]attemptView, 0, len(as))
	for _, a := range as {
		out = append(out, attemptView{
			AttemptNo: a.AttemptNo, WorkerID: a.WorkerID,
			StartedAt: a.StartedAt, EndedAt: a.EndedAt,
			Status: a.Status, Error: a.Error, ErrorCategory: a.ErrCategory,
		})
	}
	return out
}
