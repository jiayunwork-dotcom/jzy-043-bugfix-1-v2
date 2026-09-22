// Package metrics collects real-time engine metrics: queue depths, throughput,
// success/failure rates by priority, average execution latency, worker
// utilization and dead-letter backlog growth.
package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/queue"
)

// DepthSource reports ready queue lengths and wait-set sizes.
type DepthSource interface {
	AllDepths(ctx context.Context) (map[domain.Priority]int64, error)
	WaitingDepth(ctx context.Context, kind queue.WaitKind) (int64, error)
}

// WorkerStat is the compact worker view needed for utilization.
type WorkerStat struct {
	Online     bool
	TotalSlots int
	UsedSlots  int
}

// StatsSource supplies worker liveness/utilization and the dead-letter count.
type StatsSource interface {
	WorkerStats(ctx context.Context) ([]WorkerStat, error)
	DeadCount(ctx context.Context) (int, error)
}

// Collector aggregates metrics from the queue and store.
type Collector struct {
	depth DepthSource
	stats StatsSource

	startedAt time.Time
	mu        sync.Mutex
	// Per-priority outcome counters keyed by priority.
	completed map[domain.Priority]int64
	failed    map[domain.Priority]int64
	latency   map[domain.Priority]latencyAgg

	// Throughput ring (per second), and DLQ history.
	ringMu     sync.Mutex
	ringStart  time.Time
	ring       []ringPoint
	dlqHistory []DLQPoint
}

type latencyAgg struct {
	sumMS int64
	n     int64
}

type ringPoint struct {
	at                time.Time
	completed, failed int
	latencySumMS      int64
	latencyN          int
}

type dlqPoint = DLQPoint

// NewCollector builds a collector.
func NewCollector(depth DepthSource, stats StatsSource) *Collector {
	now := time.Now().UTC().Truncate(time.Second)
	c := &Collector{
		depth:     depth,
		stats:     stats,
		startedAt: now,
		completed: map[domain.Priority]int64{},
		failed:    map[domain.Priority]int64{},
		latency:   map[domain.Priority]latencyAgg{},
		ringStart: now,
		ring:      make([]ringPoint, 600),
	}
	return c
}

// RecordCompletion is called once per terminal task.
func (c *Collector) RecordCompletion(p domain.Priority, success bool, latency time.Duration) {
	c.mu.Lock()
	if success {
		c.completed[p]++
	} else {
		c.failed[p]++
	}
	la := c.latency[p]
	la.sumMS += latency.Milliseconds()
	la.n++
	c.latency[p] = la
	c.mu.Unlock()

	c.ringMu.Lock()
	now := time.Now().UTC()
	i := c.ringIdx(now)
	c.ring[i].at = now.Truncate(time.Second)
	c.ring[i].latencySumMS += latency.Milliseconds()
	c.ring[i].latencyN++
	if success {
		c.ring[i].completed++
	} else {
		c.ring[i].failed++
	}
	c.ringMu.Unlock()
}

func (c *Collector) ringIdx(t time.Time) int {
	return int(t.Truncate(time.Second).Sub(c.ringStart)/time.Second) % len(c.ring)
}

// Snapshot is the full metric payload returned to the dashboard.
type Snapshot struct {
	TakenAt           time.Time                 `json:"taken_at"`
	QueueDepths       map[string]int64          `json:"queue_depths"`
	WaitingDelay      int64                     `json:"waiting_delay"`
	WaitingRetry      int64                     `json:"waiting_retry"`
	Throughput        []ThroughputPoint         `json:"throughput"`
	CompletionsPerSec float64                   `json:"completions_per_sec"`
	PriorityStats     map[string]PriorityMetric `json:"priority_stats"`
	AvgLatencyMS      map[string]float64        `json:"avg_latency_ms"`
	Workers           WorkerMetrics             `json:"workers"`
	DLQCount          int                       `json:"dlq_count"`
	DLQGrowth         []DLQPoint                `json:"dlq_growth"`
}

// ThroughputPoint is one second of throughput.
type ThroughputPoint struct {
	T            time.Time `json:"t"`
	Completed    int       `json:"completed"`
	Failed       int       `json:"failed"`
	AvgLatencyMS float64   `json:"avg_latency_ms"`
}

// DLQPoint is one sample of dead-letter backlog.
type DLQPoint struct {
	T     time.Time `json:"t"`
	Count int       `json:"count"`
}

// PriorityMetric holds success/failure totals and rates for one priority.
type PriorityMetric struct {
	Completed   int64   `json:"completed"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"success_rate"`
	FailureRate float64 `json:"failure_rate"`
}

// WorkerMetrics summarizes the cluster.
type WorkerMetrics struct {
	Online      int     `json:"online"`
	Offline     int     `json:"offline"`
	TotalSlots  int     `json:"total_slots"`
	UsedSlots   int     `json:"used_slots"`
	Utilization float64 `json:"utilization"`
}

// Snapshot collects a point-in-time view.
func (c *Collector) Snapshot(ctx context.Context) (*Snapshot, error) {
	snap := &Snapshot{
		TakenAt:     time.Now().UTC(),
		QueueDepths: map[string]int64{},
		Throughput:  []ThroughputPoint{},
		DLQGrowth:   []DLQPoint{},
	}
	depths, err := c.depth.AllDepths(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range domain.PriorityOrder {
		snap.QueueDepths[string(p)] = depths[p]
	}
	if d, err := c.depth.WaitingDepth(ctx, queue.WaitDelay); err == nil {
		snap.WaitingDelay = d
	}
	if d, err := c.depth.WaitingDepth(ctx, queue.WaitRetry); err == nil {
		snap.WaitingRetry = d
	}

	wstats, err := c.stats.WorkerStats(ctx)
	if err != nil {
		return nil, err
	}
	for _, ws := range wstats {
		if ws.Online {
			snap.Workers.Online++
		} else {
			snap.Workers.Offline++
		}
		snap.Workers.TotalSlots += ws.TotalSlots
		if ws.Online {
			snap.Workers.UsedSlots += ws.UsedSlots
		}
	}
	if snap.Workers.TotalSlots > 0 {
		snap.Workers.Utilization = float64(snap.Workers.UsedSlots) / float64(snap.Workers.TotalSlots)
	}

	dlq, err := c.stats.DeadCount(ctx)
	if err == nil {
		snap.DLQCount = dlq
	}

	c.mu.Lock()
	snap.PriorityStats = map[string]PriorityMetric{}
	snap.AvgLatencyMS = map[string]float64{}
	for _, p := range domain.PriorityOrder {
		ok := c.completed[p]
		fail := c.failed[p]
		total := ok + fail
		m := PriorityMetric{Completed: ok, Failed: fail}
		if total > 0 {
			m.SuccessRate = float64(ok) / float64(total)
			m.FailureRate = float64(fail) / float64(total)
		}
		snap.PriorityStats[string(p)] = m
		if la := c.latency[p]; la.n > 0 {
			snap.AvgLatencyMS[string(p)] = float64(la.sumMS) / float64(la.n)
		}
	}
	c.mu.Unlock()

	c.ringMu.Lock()
	now := time.Now().UTC().Truncate(time.Second)
	for i := range c.ring {
		p := c.ring[i]
		if p.at.IsZero() {
			continue
		}
		if now.Sub(p.at) > 10*time.Minute {
			continue
		}
		tp := ThroughputPoint{T: p.at, Completed: p.completed, Failed: p.failed}
		if p.latencyN > 0 {
			tp.AvgLatencyMS = float64(p.latencySumMS) / float64(p.latencyN)
		}
		snap.Throughput = append(snap.Throughput, tp)
	}
	// DLQ growth samples are appended by the sampler loop.
	snap.DLQGrowth = append([]DLQPoint(nil), c.dlqHistory...)
	c.ringMu.Unlock()

	// Recent completions per second (last 60s).
	var sum int64
	cutoff := time.Now().Add(-60 * time.Second)
	for _, tp := range snap.Throughput {
		if tp.T.After(cutoff) {
			sum += int64(tp.Completed)
		}
	}
	snap.CompletionsPerSec = float64(sum) / 60.0

	return snap, nil
}

// SampleDLQ records one dead-letter backlog sample for the growth trend.
func (c *Collector) SampleDLQ(ctx context.Context) {
	n, err := c.stats.DeadCount(ctx)
	if err != nil {
		return
	}
	c.ringMu.Lock()
	defer c.ringMu.Unlock()
	c.dlqHistory = append(c.dlqHistory, DLQPoint{
		T: time.Now().UTC(), Count: n,
	}) // Keep ~1 hour of 5s samples.
	if len(c.dlqHistory) > 720 {
		c.dlqHistory = c.dlqHistory[len(c.dlqHistory)-720:]
	}
}
