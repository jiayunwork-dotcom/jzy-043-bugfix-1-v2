package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/asyncflow/engine/internal/api"
	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/engine"
	"github.com/asyncflow/engine/internal/metrics"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/queue"
	"github.com/asyncflow/engine/internal/runtime"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
)

// fakeExtMgr is a minimal ExternalRegistrar returning a controllable claim.
type fakeExtMgr struct {
	claims map[string]*fakeClaim
}

type fakeClaim struct {
	id     string
	taskCh chan *domain.Task
}

func (f *fakeExtMgr) Register(_ context.Context, id, _ string, _ []string, _ int) (runtime.ClaimHandle, error) {
	c := &fakeClaim{id: id, taskCh: make(chan *domain.Task, 4)}
	f.claims[id] = c
	return c, nil
}
func (f *fakeExtMgr) Heartbeat(context.Context, string, []string) error { return nil }
func (f *fakeExtMgr) Get(id string) runtime.ClaimHandle                 { return f.claims[id] }
func (f *fakeExtMgr) TaskStarted(string, *domain.Task)                  {}
func (f *fakeExtMgr) TaskFinished(string, string)                       {}
func (f *fakeExtMgr) SnapshotIDs(string) []string                       { return nil }

func (c *fakeClaim) ID() string { return c.id }
func (c *fakeClaim) PollClaim(ctx context.Context, wait time.Duration) *domain.Task {
	select {
	case t := <-c.taskCh:
		return t
	case <-time.After(wait):
		return nil
	case <-ctx.Done():
		return nil
	}
}
func (c *fakeClaim) ReleaseSlot(string) {}

func newTestApp(t *testing.T) (*fiber.App, *testutil.FakeStore, *queue.MemoryQueue) {
	t.Helper()
	fs := testutil.NewFakeStore()
	mq := queue.NewMemoryQueue()
	eng := engine.New(fs, mq, nil, nil)
	dagEng := orchestration.New(fs, testSubmitter{eng: eng, store: fs})
	collect := metrics.NewCollector(mq, fs)
	ext := &fakeExtMgr{claims: map[string]*fakeClaim{}}

	app := fiber.New()
	srv := api.NewServer(api.Deps{
		Store: fs, Engine: eng, DAG: dagEng, Metrics: collect,
		ExtMgr: ext, Queue: mq,
	})
	srv.Register(app)
	return app, fs, mq
}

type testSubmitter struct {
	eng   *engine.Engine
	store *testutil.FakeStore
}

func (s testSubmitter) SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error) {
	prio := def.Priority
	if prio == "" {
		prio = domain.PriorityNormal
	}
	t, _, err := s.eng.Submit(ctx, engine.SubmitInput{
		Type: def.Type, Payload: def.Payload, Priority: prio,
		MaxRetries: def.MaxRetries, TimeoutSeconds: def.TimeoutSecs,
		RetryPolicy: def.RetryPolicy,
	})
	if err != nil {
		return "", err
	}
	if err := s.store.LinkDAGNode(ctx, t.ID, dagID, nodeID); err != nil {
		return "", err
	}
	return t.ID, nil
}

func doJSON(t *testing.T, app *fiber.App, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, 200000)
	require.NoError(t, err)
	var out map[string]any
	if resp.Body != nil {
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if out == nil {
			out = map[string]any{}
		}
	}
	return resp.StatusCode, out
}

// TestAPITaskSubmitListFilter: end-to-end submission and filtered listing.
func TestAPITaskSubmitListFilter(t *testing.T) {
	app, fs, _ := newTestApp(t)

	status, body := doJSON(t, app, "POST", "/api/tasks", map[string]any{
		"type": "sample.echo", "payload": map[string]any{"x": 1},
		"priority": "high", "max_retries": 2, "timeout_seconds": 30,
	})
	require.Equal(t, 201, status)
	task := body["task"].(map[string]any)
	id := task["id"].(string)
	require.Equal(t, "high", task["priority"])
	require.Equal(t, "ready", task["status"])

	// Filter by priority.
	_, list := doJSON(t, app, "GET", "/api/tasks?priority=high", nil)
	require.Equal(t, float64(1), list["total"])
	require.Len(t, list["tasks"].([]any), 1)

	// Filter by an unrelated priority -> empty.
	_, empty := doJSON(t, app, "GET", "/api/tasks?priority=bulk", nil)
	require.Equal(t, float64(0), empty["total"])

	// Timeline endpoint returns attempts + audit.
	_, tl := doJSON(t, app, "GET", "/api/tasks/"+id+"/timeline", nil)
	require.NotNil(t, tl["task"])
	require.NotNil(t, tl["audit"])

	_ = fs
}

// TestAPICycleRejected422: submitting a cyclic DAG via HTTP returns 422.
func TestAPICycleRejected422(t *testing.T) {
	app, _, _ := newTestApp(t)
	body := map[string]any{
		"name": "cyc", "failure_policy": "terminate",
		"nodes": []map[string]any{
			{"id": "A", "type": "t", "dependencies": []string{"C"}},
			{"id": "B", "type": "t", "dependencies": []string{"A"}},
			{"id": "C", "type": "t", "dependencies": []string{"B"}},
		},
	}
	status, resp := doJSON(t, app, "POST", "/api/dags/validate", body)
	require.Equal(t, 422, status)
	require.NotEmpty(t, resp["cycle"])
}

// TestAPIDAGSubmitAndView: a valid DAG submits, roots launch, graph renders.
func TestAPIDAGSubmitAndView(t *testing.T) {
	app, _, _ := newTestApp(t)
	body := map[string]any{
		"name": "fan", "failure_policy": "terminate",
		"nodes": []map[string]any{
			{"id": "A", "type": "sample.echo", "priority": "high", "dependencies": []string{}},
			{"id": "B", "type": "sample.echo", "dependencies": []string{"A"}},
			{"id": "C", "type": "sample.echo", "dependencies": []string{"A"}},
			{"id": "D", "type": "sample.echo", "dependencies": []string{"B", "C"}},
		},
	}
	status, resp := doJSON(t, app, "POST", "/api/dags", body)
	require.Equal(t, 201, status)
	dagID := resp["dag_id"].(string)

	status, detail := doJSON(t, app, "GET", "/api/dags/"+dagID, nil)
	require.Equal(t, 200, status)
	layers := detail["layers"].([]any)
	require.Len(t, layers, 3) // A | B,C | D

	// The root A produced a ready task.
	_, tasks := doJSON(t, app, "GET", "/api/tasks?dag_id="+dagID, nil)
	require.GreaterOrEqual(t, tasks["total"].(float64), float64(1))
}

// TestAPIMetricsShape: the dashboard metrics endpoint returns the expected
// queues and worker structure even with an empty engine.
func TestAPIMetricsShape(t *testing.T) {
	app, _, _ := newTestApp(t)
	status, m := doJSON(t, app, "GET", "/api/metrics", nil)
	require.Equal(t, 200, status)
	depths := m["queue_depths"].(map[string]any)
	for _, p := range []string{"critical", "high", "normal", "low", "bulk"} {
		require.Contains(t, depths, p)
	}
	require.NotNil(t, m["workers"])
	// dlq_growth is an empty array (may serialize as nil list) before samples.
	require.Contains(t, m, "dlq_growth")
}
