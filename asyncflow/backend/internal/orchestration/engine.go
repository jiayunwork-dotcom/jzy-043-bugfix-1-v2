package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/storage"
	"github.com/google/uuid"
)

// Storer is the DAG persistence surface.
type Storer interface {
	CreateDAG(ctx context.Context, d *domain.DAG) error
	GetDAG(ctx context.Context, id string) (*domain.DAG, error)
	ListDAGs(ctx context.Context, limit int) ([]*domain.DAG, error)
	ListDAGNodes(ctx context.Context, dagID string) ([]*domain.DAGNode, error)
	UpdateDAGNodes(ctx context.Context, dagID string, agg domain.DAGStatus, updates []storage.DAGNodeUpdate) error
}

// NodeUpdate is the per-node transition payload.
type NodeUpdate = storage.DAGNodeUpdate

// TaskSubmitter creates tasks linked to a DAG node.
type TaskSubmitter interface {
	SubmitDAGTask(ctx context.Context, dagID, nodeID string, def domain.DAGNodeDef) (string, error)
}

// Engine drives a DAG instance forward as nodes finish.
type Engine struct {
	store Storer
	sub   TaskSubmitter
}

func New(store Storer, sub TaskSubmitter) *Engine {
	return &Engine{store: store, sub: sub}
}

// Submit validates the definition, persists it and launches the root nodes.
func (e *Engine) Submit(ctx context.Context, def domain.DAGDef) (*domain.DAG, error) {
	if def.FailurePolicy == "" {
		def.FailurePolicy = domain.DAGAbort
	}
	if _, err := Validate(def); err != nil {
		return nil, err
	}
	d := &domain.DAG{
		ID:            uuid.NewString(),
		Name:          def.Name,
		FailurePolicy: def.FailurePolicy,
		Status:        domain.DAGRunning,
		Def:           def,
		CreatedAt:     time.Now().UTC(),
	}
	if err := e.store.CreateDAG(ctx, d); err != nil {
		return nil, err
	}
	roots := []string{}
	for _, n := range def.Nodes {
		if len(n.Dependencies) == 0 {
			roots = append(roots, n.ID)
		}
	}
	if err := e.launchNodes(ctx, d, roots, nil); err != nil {
		return nil, err
	}
	return d, nil
}

func nodeDef(def domain.DAGDef, id string) (domain.DAGNodeDef, bool) {
	for _, n := range def.Nodes {
		if n.ID == id {
			return n, true
		}
	}
	return domain.DAGNodeDef{}, false
}

// launchNodes submits tasks for the given ready nodes and flips their state.
func (e *Engine) launchNodes(ctx context.Context, d *domain.DAG, nodeIDs []string, extraUpdates []NodeUpdate) error {
	updates := append([]NodeUpdate{}, extraUpdates...)
	for _, id := range nodeIDs {
		ndef, ok := nodeDef(d.Def, id)
		if !ok {
			continue
		}
		taskID, err := e.sub.SubmitDAGTask(ctx, d.ID, id, ndef)
		if err != nil {
			return err
		}
		updates = append(updates, NodeUpdate{NodeID: id, State: domain.NodeRunning, TaskID: taskID, Bump: true})
	}
	if len(updates) > 0 {
		if err := e.store.UpdateDAGNodes(ctx, d.ID, "", updates); err != nil {
			return err
		}
	}
	return nil
}

// OnTaskTerminal is invoked by the task engine when a DAG node task reaches a
// terminal state. It applies the failure policy, releases dependents and
// computes the aggregate DAG status.
func (e *Engine) OnTaskTerminal(ctx context.Context, t *domain.Task, succeeded bool, errMsg string) {
	if t.DAGID == "" {
		return
	}
	d, err := e.store.GetDAG(ctx, t.DAGID)
	if err != nil || d == nil {
		return
	}
	if d.Status == domain.DAGSucceeded || d.Status == domain.DAGFailed {
		return
	}
	nodes, err := e.store.ListDAGNodes(ctx, d.ID)
	if err != nil {
		return
	}
	byID := map[string]*domain.DAGNode{}
	for _, n := range nodes {
		byID[n.NodeID] = n
	}
	node, ok := byID[t.DAGNodeID]
	if !ok {
		return
	}

	if succeeded {
		node.State = domain.NodeSucceeded
	} else {
		// Node retries are exhausted (task is dead). Apply failure policy.
		switch d.FailurePolicy {
		case domain.DAGSkip:
			node.State = domain.NodeSkipped
		case domain.DAGRetry:
			// Manual-style retry of the node task itself.
			if err := e.retryNode(ctx, d, node); err == nil {
				return
			}
			node.State = domain.NodeFailed
		default: // DAGAbort
			node.State = domain.NodeFailed
			updates := []NodeUpdate{{NodeID: node.NodeID, State: domain.NodeFailed}}
			for _, other := range nodes {
				if other.NodeID == node.NodeID {
					continue
				}
				if other.State == domain.NodeWaiting || other.State == domain.NodeReady {
					updates = append(updates, NodeUpdate{NodeID: other.NodeID, State: domain.NodeSkipped})
				}
			}
			_ = e.store.UpdateDAGNodes(ctx, d.ID, domain.DAGFailed, updates)
			return
		}
	}

	ready, agg := e.progress(d, byID, node.NodeID, succeeded)
	updates := []NodeUpdate{{NodeID: node.NodeID, State: node.State}}
	if err := e.store.UpdateDAGNodes(ctx, d.ID, agg, updates); err != nil {
		return
	}
	if len(ready) > 0 && agg == "" {
		if err := e.launchNodes(ctx, d, ready, nil); err != nil {
			return
		}
	}
}

// progress returns nodes that just became ready and the aggregate DAG status
// when the run has concluded.
func (e *Engine) progress(d *domain.DAG, byID map[string]*domain.DAGNode, changedID string, changedSucceeded bool) ([]string, domain.DAGStatus) {
	var ready []string
	allDone := true
	anyFailed := false
	for _, n := range d.Def.Nodes {
		st := byID[n.ID].State
		if st == domain.NodeFailed {
			anyFailed = true
		}
		if st == domain.NodeWaiting || st == domain.NodeReady || st == domain.NodeRunning {
			allDone = false
		}
	}
	// Evaluate nodes whose deps just became satisfied.
	for _, n := range d.Def.Nodes {
		node := byID[n.ID]
		if node.State != domain.NodeWaiting {
			continue
		}
		depsDone := true
		for _, dep := range n.Dependencies {
			depNode := byID[dep]
			if depNode == nil {
				depsDone = false
				break
			}
			// A skipped dependency counts as satisfied (skip policy) only when
			// we are not aborting.
			if depNode.State != domain.NodeSucceeded && depNode.State != domain.NodeSkipped {
				depsDone = false
				break
			}
		}
		if depsDone {
			ready = append(ready, n.ID)
			node.State = domain.NodeReady
		}
	}
	if len(ready) > 0 {
		allDone = false
	}
	if allDone {
		if anyFailed {
			return ready, domain.DAGFailed
		}
		return ready, domain.DAGSucceeded
	}
	return ready, ""
}

func (e *Engine) retryNode(ctx context.Context, d *domain.DAG, node *domain.DAGNode) error {
	ndef, ok := nodeDef(d.Def, node.NodeID)
	if !ok {
		return fmt.Errorf("node def missing: %s", node.NodeID)
	}
	taskID, err := e.sub.SubmitDAGTask(ctx, d.ID, node.NodeID, ndef)
	if err != nil {
		return err
	}
	return e.store.UpdateDAGNodes(ctx, d.ID, "", []NodeUpdate{
		{NodeID: node.NodeID, State: domain.NodeRunning, TaskID: taskID, Bump: true},
	})
}

// GraphView is the structured representation used by the DAG page.
type GraphView struct {
	DAG    *domain.DAG `json:"dag"`
	Layers [][]string  `json:"layers"`
	Nodes  []GraphNode `json:"nodes"`
}

// GraphNode is one node plus its predecessor/successor edges and state.
type GraphNode struct {
	ID           string   `json:"id"`
	Type         string   `json:"type"`
	Priority     string   `json:"priority"`
	State        string   `json:"state"`
	TaskID       string   `json:"task_id"`
	Dependencies []string `json:"dependencies"`
	Successors   []string `json:"successors"`
	Attempts     int      `json:"attempts"`
}

// BuildGraph assembles a layered/edge view for the frontend.
func BuildGraph(d *domain.DAG, states []*domain.DAGNode) (*GraphView, error) {
	layers, err := Validate(d.Def)
	if err != nil {
		return nil, err
	}
	stateByID := map[string]*domain.DAGNode{}
	for _, s := range states {
		stateByID[s.NodeID] = s
	}
	succ := map[string][]string{}
	nodes := make([]GraphNode, 0, len(d.Def.Nodes))
	for _, n := range d.Def.Nodes {
		for _, dep := range n.Dependencies {
			succ[dep] = append(succ[dep], n.ID)
		}
	}
	for _, n := range d.Def.Nodes {
		gn := GraphNode{
			ID: n.ID, Type: n.Type, Priority: string(n.Priority),
			Dependencies: n.Dependencies, Successors: succ[n.ID],
		}
		if st, ok := stateByID[n.ID]; ok {
			gn.State = string(st.State)
			gn.TaskID = st.TaskID
			gn.Attempts = st.Attempts
		}
		nodes = append(nodes, gn)
	}
	return &GraphView{DAG: d, Layers: layers, Nodes: nodes}, nil
}

// MarshalDef is a small helper exported for HTTP handlers/tests.
func MarshalDef(def domain.DAGDef) ([]byte, error) { return json.Marshal(def) }
