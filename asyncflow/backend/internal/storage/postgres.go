// Package storage implements the Postgres-backed authoritative state store.
package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConflict means a compare-and-swap transition lost the race or the
// entity is not in the expected state.
var ErrConflict = errors.New("state transition conflict")

// Store is the Postgres persistence layer.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and waits for it to become reachable.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for i := 0; i < 60; i++ {
		if err := pool.Ping(ctx); err == nil {
			return &Store{pool: pool}, nil
		} else {
			lastErr = err
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	return nil, fmt.Errorf("postgres unreachable: %w", lastErr)
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies every *.sql file found under migrationsDir, in name order.
func (s *Store) Migrate(ctx context.Context, migrationsDir string) error {
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	var names []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		sqlBytes, err := os.ReadFile(filepath.Join(migrationsDir, name))
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply %s: %w", name, err)
		}
	}
	return nil
}

// withTx runs fn inside a transaction, committing on nil error.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// now is overridable in a tiny way for callers that need stability.
func nowUTC() time.Time { return time.Now().UTC() }
