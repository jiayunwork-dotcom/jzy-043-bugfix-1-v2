// Package queue provides the ready queues plus the delay/retry waiting areas.
// Redis is the coordination fabric; Postgres remains the authoritative task
// state. An in-memory implementation is provided for tests.
package queue

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// ReadyItem is a task that became ready from a waiting area.
type ReadyItem struct {
	TaskID   string
	Priority domain.Priority
	Head     bool // preempted/reclaimed tasks go to the head
}

// Queue is the scheduling data-plane.
type Queue interface {
	// Enqueue puts a ready task at the tail. Duplicate membership is ignored
	// (a task id is present at most once per ready queue).
	Enqueue(ctx context.Context, p domain.Priority, taskID string) error
	// EnqueueHead puts a preempted/reclaimed task back at the head.
	EnqueueHead(ctx context.Context, p domain.Priority, taskID string) error
	// Dequeue removes one task id from a priority queue; "" when empty.
	Dequeue(ctx context.Context, p domain.Priority) (string, error)
	// Depth returns the length of one ready queue.
	Depth(ctx context.Context, p domain.Priority) (int64, error)
	// AllDepths returns all five ready queue lengths.
	AllDepths(ctx context.Context) (map[domain.Priority]int64, error)

	// AddWaiting places a task in a delay/retry sorted set scored by the
	// unix second it becomes due.
	AddWaiting(ctx context.Context, kind WaitKind, taskID string, dueAt time.Time) error
	// RemoveWaiting removes a task from a waiting set (after promotion).
	RemoveWaiting(ctx context.Context, kind WaitKind, taskID string) error
	// PopDue removes and returns up to limit due task ids from a wait set.
	PopDue(ctx context.Context, kind WaitKind, now time.Time, limit int64) ([]string, error)
	// WaitingDepth reports how many entries are in a wait set.
	WaitingDepth(ctx context.Context, kind WaitKind) (int64, error)
}

// WaitKind distinguishes delayed submissions from scheduled retries.
type WaitKind string

const (
	WaitDelay WaitKind = "delay"
	WaitRetry WaitKind = "retry"
)
