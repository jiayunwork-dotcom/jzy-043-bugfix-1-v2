package main

import (
	"context"
	"fmt"
	"os"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/storage"
)

func main() {
	ctx := context.Background()
	s, err := storage.New(ctx, os.Getenv("TEST_DATABASE_DSN"))
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	defer s.Close()
	if err := s.Migrate(ctx, "migrations"); err != nil {
		fmt.Println("migrate:", err)
		os.Exit(1)
	}
	w := &domain.Worker{ID: "verify-w1", Name: "verify-1", TotalSlots: 4, Status: "online"}
	if err := s.UpsertWorker(ctx, w); err != nil {
		fmt.Println("upsert:", err)
		os.Exit(1)
	}
	ws, err := s.ListWorkers(ctx)
	if err != nil {
		fmt.Println("ListWorkers ERROR:", err)
		os.Exit(2)
	}
	fmt.Printf("ListWorkers OK: %d worker(s)\n", len(ws))
	g, err := s.GetWorker(ctx, "verify-w1")
	if err != nil {
		fmt.Println("GetWorker ERROR:", err)
		os.Exit(3)
	}
	fmt.Printf("GetWorker OK: id=%s name=%s status=%s heartbeat=%s\n",
		g.ID, g.Name, g.Status, g.LastHeartbeat.Format("15:04:05"))
}
