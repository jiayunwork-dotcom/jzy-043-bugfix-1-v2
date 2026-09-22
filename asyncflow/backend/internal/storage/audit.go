package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// writeAudit inserts an audit entry inside an existing transaction.
func writeAudit(ctx context.Context, tx pgx.Tx, entity, entityID, action, from, to, actor, detail string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO audit_logs(entity, entity_id, action, from_state, to_state, actor, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		entity, entityID, action, from, to, actor, detail)
	return err
}

// Audit writes an audit entry using its own connection (outside a tx).
func (s *Store) Audit(ctx context.Context, entity, entityID, action, from, to, actor, detail string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO audit_logs(entity, entity_id, action, from_state, to_state, actor, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		entity, entityID, action, from, to, actor, detail)
	return err
}

// ListAudit returns audit entries for an entity or globally when entityID is "".
func (s *Store) ListAudit(ctx context.Context, entity, entityID string, limit int) ([]map[string]any, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	q := `SELECT id, entity, entity_id, action, from_state, to_state, actor, detail, created_at
		FROM audit_logs WHERE ($1='' OR entity=$1) AND ($2='' OR entity_id=$2)
		ORDER BY id DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, entity, entityID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var ent, eid, action, from, to, actor, detail string
		var createdAt interface{}
		if err := rows.Scan(&id, &ent, &eid, &action, &from, &to, &actor, &detail, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "entity": ent, "entity_id": eid, "action": action,
			"from_state": from, "to_state": to, "actor": actor,
			"detail": detail, "created_at": createdAt,
		})
	}
	return out, rows.Err()
}
