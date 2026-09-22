// Command smoke runs the whole engine over an in-memory Postgres fake plus
// an embedded miniredis, so the full submit -> schedule -> execute -> retry/
// dead-letter -> DAG path can be exercised locally with no Docker/database.
//
//	go run ./cmd/smoke
//
// It seeds tasks (including a flaky one and a DAG), serves the real HTTP API
// on :8080 and prints a progress summary after a few seconds.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/asyncflow/engine/internal/api"
	"github.com/asyncflow/engine/internal/delayscheduler"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/reaper"
	"github.com/asyncflow/engine/internal/scheduler"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/asyncflow/engine/internal/workerpool"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In-memory Redis + Postgres fake.
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	rq := queue.NewRedisQueue(rdb)

	fs := testutil.NewFakeStore()
	eng := engine.New(fs, rq, nil, nil)
	collect := metrics.NewCollector(rq, fs)

	// Embedded worker: echo + flaky (fails first two attempts) + slow.
	worker := workerpool.New(workerpool.Options{
		ID: "smoke-worker", Name: "smoke-worker", Slots: 4,
		Handlers: workerpool.Handlers{
			"sample.echo":    echoH,
			"sample.flaky":   flaky,
			"sample.slow":    slow,
			"sample.compute": compute,
			"*":              echoH,
		},
		Reporter:     smokeReporter{eng: eng, collect: collect},
		Renewer:      eng,
		LeaseSeconds: 30,
	})

	// DAG engine wired back into the task engine.
	dagEng := orchestration.New(fs, smokeSubmitter{eng: eng, store: fs})
	eng.SetDAGHook(smokeHook{dag: dagEng})

	// Scheduler / delay / reaper loops.
	sched := scheduler.New(fs, rq, 30*time.Second, 3)
	sched.AddExecutor(worker)
	delay := delayscheduler.New(fs, rq, 100*time.Millisecond)
	rp := reaper.New(fs, rq, nil, time.Minute)

	go sched.Run(ctx, 20*time.Millisecond)
	go delay.Run(ctx)
	go rp.Run(ctx, 5*time.Second)
	go heartbeat(worker, fs)

	// Register the worker and seed traffic.
	_ = fs.UpsertWorker(ctx, &domain.Worker{
		ID: "smoke-worker", Name: "smoke-worker",
		Capabilities: []string{"*"}, TotalSlots: 4, Status: "online",
	})
	seed(ctx, eng, dagEng)

	// Real HTTP API.
	app := fiber.New()
	srv := api.NewServer(api.Deps{
		Store: fs, Engine: eng, DAG: dagEng, Metrics: collect, Queue: rq,
	})
	srv.Register(app)

	fmt.Println("smoke engine listening on http://localhost:8080")
	fmt.Println("  dashboard metrics:  GET http://localhost:8080/api/metrics")
	fmt.Println("  tasks:              GET http://localhost:8080/api/tasks")
	fmt.Println("  dead letters:       GET http://localhost:8080/api/dead")
	fmt.Println("seeded: echo x3, flaky(retries->success), doomed(dead), one DAG(A->B,C->D)")

	// Let the pipeline settle, then print a summary and exit.
	go func() {
		time.Sleep(6 * time.Second)
		report(ctx, fs)
		cancel()
		os.Exit(0)
	}()

	if err := app.Listen(":8080"); err != nil {
		fmt.Println("server stopped:", err)
	}
}

func heartbeat(w *workerpool.Worker, fs *testutil.FakeStore) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		s := w.SnapshotForHeartbeat()
		_ = fs.Heartbeat(context.Background(), &domain.Worker{
			ID: s.ID, Name: s.Name, Capabilities: s.Capabilities,
			TotalSlots: s.TotalSlots, UsedSlots: s.UsedSlots,
			CurrentTaskIDs: s.CurrentTasks, Status: "online",
		})
	}
}

func seed(ctx context.Context, eng *engine.Engine, dagEng *orchestration.Engine) {
	for i := 0; i < 3; i++ {
		_, _, _ = eng.Submit(ctx, engine.SubmitInput{
			Type: "sample.echo", Priority: domain.PriorityNormal, TimeoutSeconds: 10,
			Payload: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		})
	}
	// Flaky: fails twice then succeeds (max 3 retries, zero delay in smoke).
	_, _, _ = eng.Submit(ctx, engine.SubmitInput{
		Type: "sample.flaky", Priority: domain.PriorityHigh, TimeoutSeconds: 10,
		MaxRetries:  3,
		RetryPolicy: domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 0, MaxRetries: 3},
	})
	// Doomed: no retries allowed -> dead letter immediately.
	_, _, _ = eng.Submit(ctx, engine.SubmitInput{
		Type: "sample.doomed", Priority: domain.PriorityLow, TimeoutSeconds: 10,
		MaxRetries:  0,
		RetryPolicy: domain.RetryPolicy{Kind: domain.RetryFixed, BaseInterval: 0, MaxRetries: 0},
	})
	// A small fan-out/fan-in DAG.
	_, _ = dagEng.Submit(ctx, domain.DAGDef{
		Name: "smoke-dag", FailurePolicy: domain.DAGAbort,
		Nodes: []domain.DAGNodeDef{
			{ID: "A", Type: "sample.echo", Priority: domain.PriorityHigh, TimeoutSecs: 10},
			{ID: "B", Type: "sample.compute", Priority: domain.PriorityNormal, TimeoutSecs: 10, Dependencies: []string{"A"}},
			{ID: "C", Type: "sample.compute", Priority: domain.PriorityNormal, TimeoutSecs: 10, Dependencies: []string{"A"}},
			{ID: "D", Type: "sample.echo", Priority: domain.PriorityLow, TimeoutSecs: 10, Dependencies: []string{"B", "C"}},
		},
	})
}

func report(ctx context.Context, fs *testutil.FakeStore) {
	all, total, _ := fs.ListAllTasks()
	counts := map[domain.TaskStatus]int{}
	for _, t := range all {
		counts[t.Status]++
	}
	dlq, _ := fs.CountDead(ctx)
	dagList, _ := fs.ListDAGs(ctx, 5)
	fmt.Println("\n==== smoke summary ====")
	fmt.Printf("total tasks: %d\n", total)
	for _, p := range []domain.TaskStatus{
		domain.StatusReady, domain.StatusRunning, domain.StatusSucceeded,
		domain.StatusPending, domain.StatusFailed, domain.StatusDead,
	} {
		fmt.Printf("  %-10s %d\n", p, counts[p])
	}
	fmt.Printf("dead-letter backlog: %d\n", dlq)
	for _, d := range dagList {
		fmt.Printf("DAG %-12s status=%s\n", d.Name, d.Status)
	}
	if dlq >= 1 && counts[domain.StatusSucceeded] >= 4 {
		fmt.Println("RESULT: OK (flaky retried to success, doomed reached DLQ, DAG progressed)")
	} else {
		fmt.Println("RESULT: UNEXPECTED")
	}
}

// ---- adapters / handlers ----

type smokeReporter struct {
	eng     *engine.Engine
	collect *metrics.Collector
}

func (m smokeReporter) ReportResult(ctx context.Context, t *domain.Task, ok bool, res []byte, msg, cat string) {
	m.eng.ReportResult(ctx, t, ok, res, msg, cat)
	m.collect.RecordCompletion(t.Priority, ok, 0)
}

type smokeSubmitter struct {
	eng   *engine.Engine
	store *testutil.FakeStore
}

func (s smokeSubmitter) SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error) {
	t, _, err := s.eng.Submit(ctx, engine.SubmitInput{
		Type: def.Type, Payload: def.Payload, Priority: def.Priority,
		MaxRetries: def.MaxRetries, TimeoutSeconds: def.TimeoutSecs,
		RetryPolicy: def.RetryPolicy,
	})
	if err != nil {
		return "", err
	}
	return t.ID, s.store.LinkDAGNode(ctx, t.ID, dagID, nodeID)
}

type smokeHook struct{ dag *orchestration.Engine }

func (h smokeHook) OnTaskTerminal(ctx context.Context, t *domain.Task, outcome storage.CompletionOutcome, msg string) {
	h.dag.OnTaskTerminal(ctx, t, outcome == storage.OutcomeSucceeded, msg)
}

func echoH(_ context.Context, t *domain.Task) ([]byte, error) { return t.Payload, nil }

func flaky(_ context.Context, t *domain.Task) ([]byte, error) {
	if t.Attempts < 3 {
		return nil, &workerpool.ExecError{Msg: "flaky transient failure", Cat: "external_dependency"}
	}
	return []byte(`{"ok":true}`), nil
}

func slow(ctx context.Context, _ *domain.Task) ([]byte, error) {
	select {
	case <-time.After(8 * time.Second):
		return []byte(`{"slow":true}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func compute(_ context.Context, _ *domain.Task) ([]byte, error) {
	return []byte(`{"sum":49995000}`), nil
}
