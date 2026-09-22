package runtime

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/asyncflow/engine/internal/config"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/asyncflow/engine/internal/workerpool"
)

// newEmbeddedWorker constructs the in-process executor with demo handlers.
// The engine is standalone-runnable: these handlers give every task type a
// default executor; real deployments register type-specific handlers or use
// external workers.
func newEmbeddedWorker(cfg config.Config, store *storage.Store, eng *engine.Engine, collect *metrics.Collector) *workerpool.Worker {
	reporter := metricReporter{eng: eng, collect: collect}
	heartbeat := func(snap *workerpool.Snapshot) {
		if snap.ID == "" {
			return
		}
		w := &domain.Worker{
			ID:             snap.ID,
			Name:           snap.Name,
			Capabilities:   snap.Capabilities,
			TotalSlots:     snap.TotalSlots,
			UsedSlots:      snap.UsedSlots,
			CurrentTaskIDs: snap.CurrentTasks,
			Status:         "online",
		}
		if snap.Draining {
			w.Status = "draining"
		}
		_ = store.Heartbeat(context.Background(), w)
	}

	handlers := workerpool.Handlers{
		"sample.echo":    echoHandler,
		"sample.flaky":   flakyHandler,
		"sample.slow":    slowHandler,
		"sample.compute": computeHandler,
	}
	// Wildcard handler: the embedded worker accepts any task type and runs a
	// generic echo so the standalone service always makes progress.
	handlers["*"] = echoHandler

	w := workerpool.New(workerpool.Options{
		ID:           cfg.EmbeddedWorkerName,
		Name:         cfg.EmbeddedWorkerName,
		Handlers:     handlers,
		Slots:        cfg.EmbeddedWorkerSlots,
		Reporter:     reporter,
		Renewer:      eng,
		LeaseSeconds: cfg.LeaseSeconds,
		Heartbeat:    heartbeat,
	})
	// Register immediately so it is visible even before the first tick.
	_ = store.UpsertWorker(context.Background(), &domain.Worker{
		ID:           cfg.EmbeddedWorkerName,
		Name:         cfg.EmbeddedWorkerName,
		Capabilities: []string{"*", "sample.echo", "sample.flaky", "sample.slow", "sample.compute"},
		TotalSlots:   cfg.EmbeddedWorkerSlots,
		Status:       "online",
	})
	return w
}

// echoHandler returns the payload as the result.
func echoHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	return t.Payload, nil
}

// flakyHandler fails the first two attempts, succeeds on the third — used to
// demonstrate the retry pipeline end to end.
func flakyHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	if t.Attempts < 3 {
		return nil, &workerpool.ExecError{Msg: fmt.Sprintf("flaky transient failure attempt=%d", t.Attempts), Cat: "external_dependency"}
	}
	return []byte(`{"ok":true}`), nil
}

// slowHandler sleeps to demonstrate leases / preemption, honoring ctx cancel.
func slowHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	select {
	case <-time.After(8 * time.Second):
		return []byte(`{"slow":true}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// computeHandler does a tiny deterministic computation.
func computeHandler(ctx context.Context, t *domain.Task) ([]byte, error) {
	n := 1000 + rand.Intn(9000)
	sum := 0
	for i := 0; i < n; i++ {
		sum += i
	}
	return []byte(fmt.Sprintf(`{"n":%d,"sum":%d}`, n, sum)), nil
}
