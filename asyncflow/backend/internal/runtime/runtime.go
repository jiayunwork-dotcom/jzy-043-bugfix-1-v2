// Package runtime assembles every engine component and owns process lifecycle.
package runtime

import (
	"context"
	"time"

	"github.com/asyncflow/engine/internal/config"
	"github.com/asyncflow/engine/internal/delayscheduler"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/reaper"
	"github.com/asyncflow/engine/internal/scheduler"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/asyncflow/engine/internal/workerpool"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Runtime holds the wired singletons.
type Runtime struct {
	Cfg     config.Config
	Store   *storage.Store
	Queue   *queue.RedisQueue
	Engine  *engine.Engine
	Sched   *scheduler.Scheduler
	DAG     *orchestration.Engine
	Delay   *delayscheduler.Runner
	Reaper  *reaper.Reaper
	Metrics *metrics.Collector
	ExtMgr  *ExternalManager

	embedded []*workerpool.Worker
	cancel   context.CancelFunc
}

// dagSubmitter adapts the DAG engine to the task engine.
type dagSubmitter struct {
	eng *engine.Engine
}

// SubmitDAGTask creates a task bound to a DAG node.
func (d dagSubmitter) SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error) {
	prio := def.Priority
	if prio == "" {
		prio = domain.PriorityNormal
	}
	t, _, err := d.eng.Submit(ctx, engine.SubmitInput{
		Type:           def.Type,
		Payload:        def.Payload,
		Priority:       prio,
		MaxRetries:     def.MaxRetries,
		TimeoutSeconds: def.TimeoutSecs,
		RetryPolicy:    def.RetryPolicy,
		CallbackURL:    def.CallbackURL,
	})
	if err != nil {
		return "", err
	}
	t.DAGID = dagID
	t.DAGNodeID = nodeID
	return t.ID, nil
}

// Note: dag linkage is set in storage.CreateTask path via a dedicated method;
// the submitter above relies on LinkDAGNode below being applied inside the
// engine submit. We expose a small wrapper engine method via LinkNode.
type linkSubmitter struct {
	inner *dagSubmitter
	store *storage.Store
}

func (l linkSubmitter) SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error) {
	id, err := l.inner.SubmitDAGTask(ctx, dagID, nodeID, def)
	if err != nil {
		return "", err
	}
	return id, l.store.LinkDAGNode(ctx, id, dagID, nodeID)
}

// metricReporter feeds the metrics collector after every terminal attempt and
// implements workerpool.Reporter.
type metricReporter struct {
	eng     *engine.Engine
	collect *metrics.Collector
}

func (m metricReporter) ReportResult(ctx context.Context, t *domain.Task, success bool, result []byte, errMsg, category string) {
	start := time.Now()
	m.eng.ReportResult(ctx, t, success, result, errMsg, category)
	latency := time.Since(start)
	if t.StartedAt != nil {
		latency = time.Since(*t.StartedAt)
	}
	m.collect.RecordCompletion(t.Priority, success, latency)
}

// Build wires everything without starting loops.
func Build(ctx context.Context, cfg config.Config, migrationsDir string) (*Runtime, error) {
	store, err := storage.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx, migrationsDir); err != nil {
		return nil, err
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{cfg.RedisAddr},
	})
	rq := queue.NewRedisQueue(rdb)
	for i := 0; i < 30; i++ {
		if err := rq.Ping(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	rt := &Runtime{Cfg: cfg, Store: store, Queue: rq}
	rt.ExtMgr = NewExternalManager(store, rq, cfg)

	// Task engine first (DAG hooks it later).
	taskEngine := engine.New(store, rq, nil, engine.NewCallbackPoster())
	rt.Engine = taskEngine

	// Metrics collector.
	rt.Metrics = metrics.NewCollector(rq, storeMetricsAdapter{store: store})

	// Scheduler + reaper + delay.
	lease := time.Duration(cfg.LeaseSeconds) * time.Second
	rt.Sched = scheduler.New(store, rq, lease, cfg.FairnessHighWatermark)
	rt.DAG = orchestration.New(store, linkSubmitter{
		inner: &dagSubmitter{eng: taskEngine}, store: store,
	})
	taskEngine.SetDAGHook(dagHookAdapter{dag: rt.DAG})

	rt.Delay = delayscheduler.New(store, rq, cfg.DelayScanInterval)
	rt.Reaper = reaper.New(store, rq, rt.ExtMgr, time.Duration(cfg.HeartbeatTimeoutSecs)*time.Second)

	// Embedded worker with sample handlers so the service runs standalone.
	if cfg.EmbeddedWorkerEnabled {
		w := newEmbeddedWorker(cfg, store, taskEngine, rt.Metrics)
		rt.embedded = append(rt.embedded, w)
		rt.Sched.AddExecutor(w)
	}

	// External workers register via API and are executors in the scheduler.
	rt.ExtMgr.OnRegister = func(ex *ExternalWorker) { rt.Sched.AddExecutor(ex) }
	rt.ExtMgr.OnRemove = func(id string) { rt.Sched.RemoveExecutor(id) }

	return rt, nil
}

// Start recovers state and launches background loops.
func (r *Runtime) Start(ctx context.Context) error {
	// Startup reconciliation: no lost/duplicated work after a crash.
	readyTasks, reclaimed, err := r.Store.RecoverInflight(ctx)
	if err != nil {
		return err
	}
	for _, t := range readyTasks {
		_ = r.Queue.Enqueue(ctx, t.Priority, t.ID)
	}
	if err := r.Delay.RebuildWaitSets(ctx); err != nil {
		return err
	}
	_ = reclaimed

	bg, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go r.Sched.Run(bg, r.Cfg.DispatchInterval)
	go r.Delay.Run(bg)
	go r.Reaper.Run(bg, r.Cfg.ReaperInterval)
	go r.metricSampler(bg)

	for _, w := range r.embedded {
		w := w
		go w.HeartbeatLoop(bg, 5*time.Second)
	}
	go r.ExtMgr.RunHeartbeatGC(bg, r.Cfg.HeartbeatTimeoutSecs)
	return nil
}

func (r *Runtime) metricSampler(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.Metrics.SampleDLQ(ctx)
		}
	}
}

// Stop drains embedded workers gracefully and closes resources.
func (r *Runtime) Stop(sigCtx context.Context) {
	if r.cancel != nil {
		r.cancel()
	}
	for _, w := range r.embedded {
		w.Drain(sigCtx)
	}
	r.Store.Close()
}

// newID exposes uuid for external worker ids.
func newID() string { return uuid.NewString() }
