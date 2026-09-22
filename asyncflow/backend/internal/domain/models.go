// Package domain holds the core entities shared across the engine.
package domain

import (
	"context"
	"time"
)

// Priority is one of five fixed scheduling classes, highest first.
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
	PriorityBulk     Priority = "bulk"
)

// PriorityOrder lists priorities from highest to lowest; index is the rank.
var PriorityOrder = []Priority{
	PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow, PriorityBulk,
}

// Rank returns a numeric rank where 0 is the highest priority.
func (p Priority) Rank() int {
	for i, q := range PriorityOrder {
		if p == q {
			return i
		}
	}
	return len(PriorityOrder)
}

func (p Priority) Valid() bool { return p.Rank() < len(PriorityOrder) }

// Task status values across the lifecycle.
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"   // in delay / retry waiting area
	StatusReady     TaskStatus = "ready"     // in a priority ready queue
	StatusRunning   TaskStatus = "running"   // leased by a worker
	StatusSucceeded TaskStatus = "succeeded" // terminal success
	StatusFailed    TaskStatus = "failed"    // final failure (DAG skip / abort)
	StatusDead      TaskStatus = "dead"      // retries exhausted -> dead letter
	StatusCanceled  TaskStatus = "canceled"  // terminal cancel
	StatusPaused    TaskStatus = "paused"    // interrupted by preemption, requeue pending
)

var TerminalStatuses = map[TaskStatus]bool{
	StatusSucceeded: true,
	StatusFailed:    true,
	StatusDead:      true,
	StatusCanceled:  true,
}

// RetryKind enumerates retry strategies.
type RetryKind string

const (
	RetryExponential RetryKind = "exponential"
	RetryFixed       RetryKind = "fixed"
	RetryCron        RetryKind = "cron"
)

// RetryPolicy describes how a task is retried after a failed attempt.
type RetryPolicy struct {
	Kind           RetryKind `json:"kind"`
	BaseInterval   int       `json:"base_interval_seconds"` // exponential base / fixed interval
	MaxRetries     int       `json:"max_retries"`
	CronExpression string    `json:"cron_expression,omitempty"` // kind=cron
}

// NextDelay computes seconds until the next attempt given attempts already made.
// attemptNumber is 1-based (the attempt that just failed).
func (r RetryPolicy) NextDelay(attemptNumber int, now time.Time) (int, error) {
	switch r.Kind {
	case RetryFixed:
		return r.BaseInterval, nil
	case RetryExponential, "":
		d := r.BaseInterval
		if d <= 0 {
			d = 1
		}
		// base * 2^(attempt-1), capped to keep numbers sane.
		for i := 1; i < attemptNumber; i++ {
			if d >= 3600 {
				break
			}
			d *= 2
		}
		return d, nil
	case RetryCron:
		sched, err := ParseCron(r.CronExpression)
		if err != nil {
			return 0, err
		}
		next := sched.Next(now)
		return int(next.Sub(now).Seconds()) + 1, nil
	default:
		return r.BaseInterval, nil
	}
}

// Task is the persisted unit of work.
type Task struct {
	ID             string      `json:"id"`
	IDempotencyKey string      `json:"idempotency_key,omitempty"`
	Type           string      `json:"type"`
	Payload        []byte      `json:"payload"`
	Priority       Priority    `json:"priority"`
	Status         TaskStatus  `json:"status"`
	MaxRetries     int         `json:"max_retries"`
	Attempts       int         `json:"attempts"`
	TimeoutSeconds int         `json:"timeout_seconds"`
	RetryPolicy    RetryPolicy `json:"retry_policy"`
	CallbackURL    string      `json:"callback_url,omitempty"`
	ExecuteAfter   *time.Time  `json:"execute_after,omitempty"`
	WorkerID       string      `json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time  `json:"lease_expires_at,omitempty"`
	// DAG linkage; empty for standalone tasks.
	DAGID       string `json:"dag_id,omitempty"`
	DAGNodeID   string `json:"dag_node_id,omitempty"`
	LastError   string `json:"last_error,omitempty"`
	ErrCategory string `json:"error_category,omitempty"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// Attempt records one execution of a task.
type Attempt struct {
	ID          string
	TaskID      string
	AttemptNo   int
	WorkerID    string
	StartedAt   time.Time
	EndedAt     *time.Time
	Status      string // running, succeeded, failed, timeout, interrupted
	Error       string
	ErrCategory string
	Result      []byte
}

// Worker is a registered executor process.
type Worker struct {
	ID             string
	Name           string
	Capabilities   []string // task types supported
	TotalSlots     int
	UsedSlots      int
	Status         string // online, draining, offline
	LastHeartbeat  time.Time
	CurrentTaskIDs []string
	Completed      int64
	Failed         int64
	CreatedAt      time.Time
}

// DAGFailurePolicy defines behavior when a DAG node fails its retries.
type DAGFailurePolicy string

const (
	DAGAbort DAGFailurePolicy = "terminate"
	DAGSkip  DAGFailurePolicy = "skip"
	DAGRetry DAGFailurePolicy = "retry"
)

// DAGNodeDef is a node in a submitted DAG definition.
type DAGNodeDef struct {
	ID           string      `json:"id"`
	Type         string      `json:"type"`
	Payload      []byte      `json:"payload"`
	Priority     Priority    `json:"priority"`
	TimeoutSecs  int         `json:"timeout_seconds"`
	MaxRetries   int         `json:"max_retries"`
	RetryPolicy  RetryPolicy `json:"retry_policy"`
	CallbackURL  string      `json:"callback_url,omitempty"`
	Dependencies []string    `json:"dependencies"`
}

// DAGDef is a submitted workflow definition.
type DAGDef struct {
	Name          string           `json:"name"`
	FailurePolicy DAGFailurePolicy `json:"failure_policy"`
	Nodes         []DAGNodeDef     `json:"nodes"`
}

// DAG aggregate status values.
type DAGStatus string

const (
	DAGPending   DAGStatus = "pending"
	DAGRunning   DAGStatus = "running"
	DAGSucceeded DAGStatus = "succeeded"
	DAGFailed    DAGStatus = "failed"
)

// DAG is a running workflow instance.
type DAG struct {
	ID            string
	Name          string
	FailurePolicy DAGFailurePolicy
	Status        DAGStatus
	Def           DAGDef
	CreatedAt     time.Time
	FinishedAt    *time.Time
}

// DAGNodeState is the per-node runtime state.
type DAGNodeState string

const (
	NodeWaiting   DAGNodeState = "waiting"
	NodeReady     DAGNodeState = "ready"
	NodeRunning   DAGNodeState = "running"
	NodeSucceeded DAGNodeState = "succeeded"
	NodeFailed    DAGNodeState = "failed"
	NodeSkipped   DAGNodeState = "skipped"
)

// DAGNode ties a node definition to runtime state.
type DAGNode struct {
	DAGID        string
	NodeID       string
	TaskID       string
	State        DAGNodeState
	Dependencies []string
	Attempts     int
	UpdatedAt    time.Time
}

// AuditEntry is an immutable state-change record.
type AuditEntry struct {
	ID        int64
	Entity    string // task, dag, worker
	EntityID  string
	Action    string
	FromState string
	ToState   string
	Actor     string
	Detail    string
	CreatedAt time.Time
}

// DeadLetter is the projected view of a dead-lettered task.
type DeadLetter struct {
	Task     Task      `json:"task"`
	Attempts []Attempt `json:"attempts"`
	DeadAt   time.Time `json:"dead_at"`
}

// HandlerFunc executes a task payload. Returning an error fails the attempt.
// The context is canceled if the task is preempted or its lease times out.
type HandlerFunc func(ctx context.Context, t *Task) ([]byte, error)
