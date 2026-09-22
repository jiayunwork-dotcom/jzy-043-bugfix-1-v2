package api

import (
	"github.com/gofiber/fiber/v2"
)

type deadTaskView struct {
	Task     taskView      `json:"task"`
	Attempts []attemptView `json:"attempts"`
	DeadAt   string        `json:"dead_at"`
}

func (s *Server) listDead(c *fiber.Ctx) error {
	limit := atoiDefault(c.Query("limit"), 100)
	offset := atoiDefault(c.Query("offset"), 0)
	dead, total, err := s.d.Store.ListDead(c.Context(), limit, offset)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	out := make([]deadTaskView, 0, len(dead))
	for _, d := range dead {
		out = append(out, deadTaskView{
			Task:     toTaskView(&d.Task),
			Attempts: toAttemptViews(d.Attempts),
			DeadAt:   d.DeadAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}
	return c.JSON(fiber.Map{"dead": out, "total": total})
}

func (s *Server) aggregateDead(c *fiber.Ctx) error {
	rows, err := s.d.Store.AggregateDead(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"aggregates": rows})
}

type idBatch struct {
	IDs []string `json:"ids"`
}

func (s *Server) retryDead(c *fiber.Ctx) error {
	var b idBatch
	if err := c.BodyParser(&b); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if len(b.IDs) == 0 {
		return c.Status(400).JSON(fiber.Map{"error": "ids required"})
	}
	moved, n, err := s.d.Store.RetryDead(c.Context(), b.IDs)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	// Push every moved task back to its priority ready queue.
	for _, t := range moved {
		_ = s.d.Queue.Enqueue(c.Context(), t.Priority, t.ID)
	}
	return c.JSON(fiber.Map{"retried": n})
}

func (s *Server) discardDead(c *fiber.Ctx) error {
	var b idBatch
	if err := c.BodyParser(&b); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	n, err := s.d.Store.DiscardDead(c.Context(), b.IDs)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"discarded": n})
}
