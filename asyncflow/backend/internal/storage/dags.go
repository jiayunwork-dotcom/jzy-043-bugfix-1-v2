package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/jackc/pgx/v5"
)

// LinkDAGNode binds a submitted task to a DAG node.
func (s *Store) LinkDAGNode(ctx context.Context, taskID, dagID, nodeID string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE tasks SET dag_id=$2, dag_node_id=$3 WHERE id=$1`,
			taskID, dagID, nodeID)
		return err
	})
}

// CreateDAG persists a new DAG definition and its node states.
func (s *Store) CreateDAG(ctx context.Context, d *domain.DAG) error {
	defBytes, err := json.Marshal(d.Def)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO dags(id, name, failure_policy, status, definition, created_at)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			d.ID, d.Name, string(d.FailurePolicy), string(d.Status), defBytes, d.CreatedAt)
		if err != nil {
			return err
		}
		for _, n := range d.Def.Nodes {
			_, err := tx.Exec(ctx, `
				INSERT INTO dag_nodes(dag_id, node_id, state, dependencies)
				VALUES ($1,$2,'waiting',$3)`,
				d.ID, n.ID, n.Dependencies)
			if err != nil {
				return err
			}
		}
		return writeAudit(ctx, tx, "dag", d.ID, "created", "", string(d.Status), "api",
			fmt.Sprintf("nodes=%d policy=%s", len(d.Def.Nodes), d.FailurePolicy))
	})
}

// GetDAG loads a DAG definition.
func (s *Store) GetDAG(ctx context.Context, id string) (*domain.DAG, error) {
	var d domain.DAG
	var defBytes []byte
	var policy, status string
	err := s.pool.QueryRow(ctx, `
		SELECT id, name, failure_policy, status, definition, created_at, finished_at
		FROM dags WHERE id=$1`, id).Scan(
		&d.ID, &d.Name, &policy, &status, &defBytes, &d.CreatedAt, &d.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(defBytes, &d.Def); err != nil {
		return nil, err
	}
	d.FailurePolicy = domain.DAGFailurePolicy(policy)
	d.Status = domain.DAGStatus(status)
	return &d, nil
}

// ListDAGs lists recent DAG instances.
func (s *Store) ListDAGs(ctx context.Context, limit int) ([]*domain.DAG, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, failure_policy, status, definition, created_at, finished_at
		FROM dags ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DAG
	for rows.Next() {
		var d domain.DAG
		var defBytes []byte
		var policy, status string
		if err := rows.Scan(&d.ID, &d.Name, &policy, &status, &defBytes,
			&d.CreatedAt, &d.FinishedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(defBytes, &d.Def); err != nil {
			return nil, err
		}
		d.FailurePolicy = domain.DAGFailurePolicy(policy)
		d.Status = domain.DAGStatus(status)
		out = append(out, &d)
	}
	return out, rows.Err()
}

// ListDAGNodes loads node runtime states for a DAG.
func (s *Store) ListDAGNodes(ctx context.Context, dagID string) ([]*domain.DAGNode, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT dag_id, node_id, task_id, state, dependencies, attempts, updated_at
		FROM dag_nodes WHERE dag_id=$1 ORDER BY node_id`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.DAGNode
	for rows.Next() {
		var n domain.DAGNode
		if err := rows.Scan(&n.DAGID, &n.NodeID, &n.TaskID, &n.State,
			&n.Dependencies, &n.Attempts, &n.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &n)
	}
	return out, rows.Err()
}

// DAGNodeUpdate carries a node-state transition.
type DAGNodeUpdate struct {
	NodeID string
	State  domain.DAGNodeState
	TaskID string // optional
	Bump   bool   // increment attempts
}

// UpdateDAGNodes applies node updates and optionally the aggregate DAG status
// in one transaction.
func (s *Store) UpdateDAGNodes(ctx context.Context, dagID string, agg domain.DAGStatus, updates []DAGNodeUpdate) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for _, u := range updates {
			var err error
			switch {
			case u.Bump && u.TaskID != "":
				_, err = tx.Exec(ctx, `
					UPDATE dag_nodes SET state=$2, task_id=$3, attempts=attempts+1, updated_at=now()
					WHERE dag_id=$1 AND node_id=$4`, dagID, string(u.State), u.TaskID, u.NodeID)
			case u.Bump:
				_, err = tx.Exec(ctx, `
					UPDATE dag_nodes SET state=$2, attempts=attempts+1, updated_at=now()
					WHERE dag_id=$1 AND node_id=$3`, dagID, string(u.State), u.NodeID)
			case u.TaskID != "":
				_, err = tx.Exec(ctx, `
					UPDATE dag_nodes SET state=$2, task_id=$3, updated_at=now()
					WHERE dag_id=$1 AND node_id=$4`, dagID, string(u.State), u.TaskID, u.NodeID)
			default:
				_, err = tx.Exec(ctx, `
					UPDATE dag_nodes SET state=$2, updated_at=now()
					WHERE dag_id=$1 AND node_id=$3`, dagID, string(u.State), u.NodeID)
			}
			if err != nil {
				return err
			}
			if err := writeAudit(ctx, tx, "dag_node", dagID+":"+u.NodeID, "node_state",
				"", string(u.State), "engine", ""); err != nil {
				return err
			}
		}
		if agg != "" {
			var finished any
			if agg == domain.DAGSucceeded || agg == domain.DAGFailed {
				finished = "now()"
			}
			if finished != nil {
				_, err := tx.Exec(ctx,
					`UPDATE dags SET status=$2, finished_at=now() WHERE id=$1`, dagID, string(agg))
				if err != nil {
					return err
				}
			} else {
				_, err := tx.Exec(ctx,
					`UPDATE dags SET status=$2 WHERE id=$1`, dagID, string(agg))
				if err != nil {
					return err
				}
			}
			if err := writeAudit(ctx, tx, "dag", dagID, "dag_state", "", string(agg), "engine", ""); err != nil {
				return err
			}
		}
		return nil
	})
}
