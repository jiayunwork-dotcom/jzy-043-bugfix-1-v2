package orchestration_test

import (
	"context"
	"testing"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/asyncflow/engine/internal/testutil"
	"github.com/stretchr/testify/require"
)

// fakeSubmitter records every node task and persists a linked ready task so
// OnTaskTerminal can resolve the node from the task's DAG linkage.
type fakeSubmitter struct {
	store     *testutil.FakeStore
	submitted []string // node IDs in submission order
}

func (f *fakeSubmitter) SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error) {
	f.submitted = append(f.submitted, nodeID)
	taskID := "task-for-" + nodeID
	t := &domain.Task{
		ID: taskID, Type: def.Type, Priority: def.Priority,
		Status: domain.StatusReady, TimeoutSeconds: def.TimeoutSecs,
		MaxRetries: def.MaxRetries, RetryPolicy: def.RetryPolicy,
		DAGID: dagID, DAGNodeID: nodeID,
	}
	if err := f.store.CreateTask(ctx, t); err != nil {
		return "", err
	}
	return taskID, nil
}

func node(id string, deps ...string) domain.DAGNodeDef {
	return domain.DAGNodeDef{
		ID: id, Type: "sample.echo", Priority: domain.PriorityNormal,
		TimeoutSecs: 10, Dependencies: deps,
	}
}

// TestCycleDetectionRejectsCyclicDAG: A->B->C->A must be rejected with the
// cycle reported; an acyclic DAG validates.
func TestCycleDetectionRejectsCyclicDAG(t *testing.T) {
	cyclic := domain.DAGDef{
		Name: "cyc", FailurePolicy: domain.DAGAbort,
		Nodes: []domain.DAGNodeDef{
			node("A", "C"), node("B", "A"), node("C", "B"),
		},
	}
	has, cycle := orchestration.HasCycle(cyclic)
	require.True(t, has, "cycle must be detected")
	require.NotEmpty(t, cycle)

	_, err := orchestration.Validate(cyclic)
	require.Error(t, err)
	var ce *orchestration.CycleError
	require.ErrorAs(t, err, &ce)

	acyclic := domain.DAGDef{
		Name: "ok", FailurePolicy: domain.DAGAbort,
		Nodes: []domain.DAGNodeDef{node("A"), node("B", "A"), node("C", "A"), node("D", "B", "C")},
	}
	layers, err := orchestration.Validate(acyclic)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"A"}, {"B", "C"}, {"D"}}, layers)

	// Submitting a cyclic DAG through the engine is refused (422 at API layer).
	fs := testutil.NewFakeStore()
	sub := &fakeSubmitter{store: fs}
	eng := orchestration.New(fs, sub)
	_, err = eng.Submit(context.Background(), cyclic)
	require.Error(t, err)
}

// TestDAGDependencyOrder: A fans out to B and C; D runs only after B and C.
func TestDAGDependencyOrder(t *testing.T) {
	fs := testutil.NewFakeStore()
	sub := &fakeSubmitter{store: fs}
	eng := orchestration.New(fs, sub)
	ctx := context.Background()

	def := domain.DAGDef{
		Name: "fan", FailurePolicy: domain.DAGAbort,
		Nodes: []domain.DAGNodeDef{
			node("A"), node("B", "A"), node("C", "A"), node("D", "B", "C"),
		},
	}
	d, err := eng.Submit(ctx, def)
	require.NoError(t, err)
	require.Equal(t, []string{"A"}, sub.submitted, "only root A launches initially")

	states, err := fs.ListDAGNodes(ctx, d.ID)
	require.NoError(t, err)
	stateOf := map[string]domain.DAGNodeState{}
	for _, s := range states {
		stateOf[s.NodeID] = s.State
	}
	require.Equal(t, domain.NodeRunning, stateOf["A"])
	require.Equal(t, domain.NodeWaiting, stateOf["B"])

	// A succeeds -> B and C become runnable in parallel.
	aTask, _ := fs.GetTask(ctx, "task-for-A")
	eng.OnTaskTerminal(ctx, aTask, true, "")
	require.ElementsMatch(t, []string{"B", "C"}, sub.submitted[1:])

	// B succeeds alone -> D must not launch yet (C pending).
	bTask, _ := fs.GetTask(ctx, "task-for-B")
	eng.OnTaskTerminal(ctx, bTask, true, "")
	require.NotContains(t, sub.submitted, "D", "D waits for both B and C")

	// C succeeds -> D launches and DAG completes.
	cTask, _ := fs.GetTask(ctx, "task-for-C")
	eng.OnTaskTerminal(ctx, cTask, true, "")
	require.Contains(t, sub.submitted, "D")

	dTask, _ := fs.GetTask(ctx, "task-for-D")
	eng.OnTaskTerminal(ctx, dTask, true, "")

	final, err := fs.GetDAG(ctx, d.ID)
	require.NoError(t, err)
	require.Equal(t, domain.DAGSucceeded, final.Status)

	nodes, err := fs.ListDAGNodes(ctx, d.ID)
	require.NoError(t, err)
	for _, n := range nodes {
		require.Equal(t, domain.NodeSucceeded, n.State, "node %s", n.NodeID)
	}
}

// TestDAGAbortPolicy: with terminate policy, a failed node aborts the whole
// DAG and skips downstream waiting nodes.
func TestDAGAbortPolicy(t *testing.T) {
	fs := testutil.NewFakeStore()
	sub := &fakeSubmitter{store: fs}
	eng := orchestration.New(fs, sub)
	ctx := context.Background()

	def := domain.DAGDef{
		Name: "abort", FailurePolicy: domain.DAGAbort,
		Nodes: []domain.DAGNodeDef{node("A"), node("B", "A"), node("C", "B")},
	}
	d, err := eng.Submit(ctx, def)
	require.NoError(t, err)

	aTask, _ := fs.GetTask(ctx, "task-for-A")
	eng.OnTaskTerminal(ctx, aTask, true, "")
	require.Contains(t, sub.submitted, "B")

	bTask, _ := fs.GetTask(ctx, "task-for-B")
	eng.OnTaskTerminal(ctx, bTask, false, "boom")

	final, err := fs.GetDAG(ctx, d.ID)
	require.NoError(t, err)
	require.Equal(t, domain.DAGFailed, final.Status)

	nodes, err := fs.ListDAGNodes(ctx, d.ID)
	require.NoError(t, err)
	state := map[string]domain.DAGNodeState{}
	for _, n := range nodes {
		state[n.NodeID] = n.State
	}
	require.Equal(t, domain.NodeSucceeded, state["A"])
	require.Equal(t, domain.NodeFailed, state["B"])
	require.Equal(t, domain.NodeSkipped, state["C"], "downstream C is skipped on abort")
}
