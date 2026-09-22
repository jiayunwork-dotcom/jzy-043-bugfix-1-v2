package engine

import "github.com/google/uuid"

// newID generates a task/worker identifier.
func newID() string { return uuid.NewString() }
