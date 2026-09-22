package api

import (
	"strconv"
	"time"

	"github.com/asyncflow/engine/internal/storage"
	"github.com/gofiber/fiber/v2"
)

func (s *Server) submitTask(c *fiber.Ctx) error {
	var req submitTaskRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid request body: " + err.Error()})
	}
	in, err := req.toInput()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	t, duplicated, err := s.d.Engine.Submit(c.Context(), in)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	status := 201
	resp := fiber.Map{"task": toTaskView(t)}
	if duplicated {
		status = 200
		resp["duplicated"] = true
	}
	return c.Status(status).JSON(resp)
}

func (s *Server) listTasks(c *fiber.Ctx) error {
	f := storage.TaskFilter{
		Priority: c.Query("priority"),
		Type:     c.Query("type"),
		DAGID:    c.Query("dag_id"),
	}
	if st := c.Query("status"); st != "" {
		// Allow comma-separated multi-status.
		var statuses []string
		for _, x := range splitCSV(st) {
			statuses = append(statuses, x)
		}
		f.Statuses = statuses
	}
	if from := c.Query("from"); from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			f.From = &t
		}
	}
	if to := c.Query("to"); to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			f.To = &t
		}
	}
	f.Limit = atoiDefault(c.Query("limit"), 100)
	f.Offset = atoiDefault(c.Query("offset"), 0)

	tasks, total, err := s.d.Store.ListTasks(c.Context(), f)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	views := make([]taskView, 0, len(tasks))
	for _, t := range tasks {
		views = append(views, toTaskView(t))
	}
	return c.JSON(fiber.Map{"tasks": views, "total": total, "limit": f.Limit, "offset": f.Offset})
}

func (s *Server) getTask(c *fiber.Ctx) error {
	t, err := s.d.Store.GetTask(c.Context(), c.Params("id"))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if t == nil {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}
	return c.JSON(fiber.Map{"task": toTaskView(t)})
}

func (s *Server) taskTimeline(c *fiber.Ctx) error {
	id := c.Params("id")
	t, err := s.d.Store.GetTask(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if t == nil {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}
	attempts, err := s.d.Store.Attempts(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	audit, _ := s.d.Store.ListAudit(c.Context(), "task", id, 200)
	return c.JSON(fiber.Map{
		"task":     toTaskView(t),
		"attempts": toAttemptViews(attempts),
		"audit":    audit,
	})
}

func (s *Server) cancelTask(c *fiber.Ctx) error {
	id := c.Params("id")
	t, err := s.d.Store.GetTask(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if t == nil {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}
	if err := s.d.Store.CancelTask(c.Context(), id); err != nil {
		return c.Status(409).JSON(fiber.Map{"error": err.Error()})
	}
	_ = s.d.Queue.RemoveWaitingKind(c.Context(), "delay", id)
	_ = s.d.Queue.RemoveWaitingKind(c.Context(), "retry", id)
	t2, _ := s.d.Store.GetTask(c.Context(), id)
	return c.JSON(fiber.Map{"task": toTaskView(t2)})
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
