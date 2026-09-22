// Package orchestration implements DAG workflow submission, cycle detection,
// topological readiness, node state tracking and failure policies.
package orchestration

import (
	"fmt"

	"github.com/asyncflow/engine/internal/domain"
)

// CycleError describes a dependency cycle rejected at submission time.
type CycleError struct {
	Cycle []string
}

func (e *CycleError) Error() string {
	return fmt.Sprintf("dependency cycle detected: %v", e.Cycle)
}

// ValidationError describes a malformed DAG definition.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// Validate checks a DAG definition: unique node ids, dependencies that exist,
// and absence of cycles. It returns nodes in topological layers on success.
// layers[i] are nodes whose dependencies all live in earlier layers.
func Validate(def domain.DAGDef) ([][]string, error) {
	if len(def.Nodes) == 0 {
		return nil, &ValidationError{Msg: "DAG has no nodes"}
	}
	nodes := map[string]*domain.DAGNodeDef{}
	for i := range def.Nodes {
		n := &def.Nodes[i]
		if n.ID == "" {
			return nil, &ValidationError{Msg: "node id is required"}
		}
		if _, dup := nodes[n.ID]; dup {
			return nil, &ValidationError{Msg: "duplicate node id: " + n.ID}
		}
		nodes[n.ID] = n
	}
	indeg := map[string]int{}
	children := map[string][]string{}
	for _, n := range def.Nodes {
		indeg[n.ID] = len(n.Dependencies)
		for _, dep := range n.Dependencies {
			if _, ok := nodes[dep]; !ok {
				return nil, &ValidationError{Msg: fmt.Sprintf("node %s depends on unknown node %s", n.ID, dep)}
			}
			children[dep] = append(children[dep], n.ID)
		}
	}

	var layers [][]string
	remaining := len(indeg)
	frontier := []string{}
	for id, d := range indeg {
		if d == 0 {
			frontier = append(frontier, id)
		}
	}
	sortedIDs := func(ids []string) {
		// deterministic order
		for i := 1; i < len(ids); i++ {
			for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
				ids[j-1], ids[j] = ids[j], ids[j-1]
			}
		}
	}
	sortedIDs(frontier)

	for len(frontier) > 0 {
		layer := append([]string{}, frontier...)
		layers = append(layers, layer)
		var next []string
		for _, id := range layer {
			remaining--
			for _, ch := range children[id] {
				indeg[ch]--
				if indeg[ch] == 0 {
					next = append(next, ch)
				}
			}
		}
		sortedIDs(next)
		frontier = next
	}
	if remaining > 0 {
		// Nodes with residual in-degree participate in a cycle; recover one.
		var stuck []string
		for id, d := range indeg {
			if d > 0 {
				stuck = append(stuck, id)
			}
		}
		return nil, &CycleError{Cycle: stuck}
	}
	return layers, nil
}

// HasCycle is a convenience wrapper around Validate.
func HasCycle(def domain.DAGDef) (bool, []string) {
	_, err := Validate(def)
	if ce, ok := err.(*CycleError); ok {
		return true, ce.Cycle
	}
	return false, nil
}
