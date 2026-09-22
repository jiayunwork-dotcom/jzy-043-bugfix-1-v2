// Command asyncflow-server runs the full engine: HTTP API, scheduler, delay
// scanner, reaper, DAG orchestrator, metrics and an embedded worker.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/asyncflow/engine/internal/api"
	"github.com/asyncflow/engine/internal/config"
	"github.com/asyncflow/engine/internal/runtime"
	"github.com/gofiber/fiber/v2"
)

func main() {
	migrations := flag.String("migrations", "migrations", "directory containing SQL migrations")
	flag.Parse()

	absMigrations, err := filepath.Abs(*migrations)
	if err != nil {
		absMigrations = *migrations
	}

	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rt, err := runtime.Build(ctx, cfg, absMigrations)
	if err != nil {
		panic(err)
	}
	if err := rt.Start(ctx); err != nil {
		panic(err)
	}

	app := fiber.New(fiber.Config{
		AppName:               "asyncflow",
		DisableStartupMessage: false,
	})
	srv := api.NewServer(api.Deps{
		Store: rt.Store, Engine: rt.Engine, DAG: rt.DAG,
		Metrics: rt.Metrics, ExtMgr: rt.ExtMgr, Queue: rt.Queue, Delay: rt.Delay,
	})
	srv.Register(app)

	go func() {
		if err := app.Listen(cfg.HTTPAddr); err != nil {
			// Fiber returns ErrServerClosed on shutdown; ignore that.
			_ = err
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = app.ShutdownWithContext(shutdownCtx)
	rt.Stop(shutdownCtx)
	os.Exit(0)
}
