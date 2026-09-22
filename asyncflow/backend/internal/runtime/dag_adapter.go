package runtime

import (
	"context"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/storage"
)

// dagHookAdapter converts engine terminal events into the orchestration
// callback shape (success bool).
type dagHookAdapter struct {
	dag *orchestration.Engine
}

func (a dagHookAdapter) OnTaskTerminal(ctx context.Context, t *domain.Task, outcome storage.CompletionOutcome, errMsg string) {
	a.dag.OnTaskTerminal(ctx, t, outcome == storage.OutcomeSucceeded, errMsg)
}
