package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// Register mounts every API route on the Fiber app.
func (s *Server) Register(app *fiber.App) {
	app.Use(cors.New(cors.Config{AllowOrigins: "*"}))
	app.Use(logger.New())

	app.Get("/api/health", s.health)

	api := app.Group("/api")

	// Tasks.
	api.Post("/tasks", s.submitTask)
	api.Get("/tasks", s.listTasks)
	api.Get("/tasks/:id", s.getTask)
	api.Get("/tasks/:id/timeline", s.taskTimeline)
	api.Post("/tasks/:id/cancel", s.cancelTask)

	// Dead letters.
	api.Get("/dead", s.listDead)
	api.Get("/dead/aggregate", s.aggregateDead)
	api.Post("/dead/retry", s.retryDead)
	api.Post("/dead/discard", s.discardDead)

	// Workers.
	api.Get("/workers", s.listWorkers)
	api.Get("/workers/:id", s.getWorker)
	api.Post("/workers/:id/drain", s.drainWorker)

	// DAGs.
	api.Post("/dags/validate", s.validateDAG)
	api.Post("/dags", s.submitDAG)
	api.Get("/dags", s.listDAGs)
	api.Get("/dags/:id", s.getDAG)

	// Metrics & audit.
	api.Get("/metrics", s.metrics)
	api.Get("/audit", s.listAudit)

	// External worker protocol.
	api.Post("/workers/register", s.registerExternalWorker)
	api.Post("/workers/heartbeat", s.externalHeartbeat)
	api.Post("/workers/:id/claim", s.claimTask)
	api.Post("/workers/:id/complete", s.completeTask)
}

func (s *Server) health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok", "service": "asyncflow"})
}
