package runtime

import (
	"context"

	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/storage"
)

// storeMetricsAdapter exposes worker/DLQ stats for the metrics collector.
type storeMetricsAdapter struct {
	store *storage.Store
}

func (a storeMetricsAdapter) WorkerStats(ctx context.Context) ([]metrics.WorkerStat, error) {
	ws, err := a.store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]metrics.WorkerStat, 0, len(ws))
	for _, w := range ws {
		out = append(out, metrics.WorkerStat{
			Online:     w.Status == "online" || w.Status == "draining",
			TotalSlots: w.TotalSlots,
			UsedSlots:  w.UsedSlots,
		})
	}
	return out, nil
}

func (a storeMetricsAdapter) DeadCount(ctx context.Context) (int, error) {
	return a.store.CountDead(ctx)
}
