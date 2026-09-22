package api

import (
	"github.com/gofiber/fiber/v2"
)

func (s *Server) metrics(c *fiber.Ctx) error {
	snap, err := s.d.Metrics.Snapshot(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(snap)
}

func (s *Server) listAudit(c *fiber.Ctx) error {
	entity := c.Query("entity", "")
	entityID := c.Query("entity_id", "")
	limit := atoiDefault(c.Query("limit"), 200)
	entries, err := s.d.Store.ListAudit(c.Context(), entity, entityID, limit)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"audit": entries})
}
