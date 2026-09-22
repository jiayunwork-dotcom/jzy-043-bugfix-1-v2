package api

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

type workerView struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Capabilities  []string  `json:"capabilities"`
	TotalSlots    int       `json:"total_slots"`
	UsedSlots     int       `json:"used_slots"`
	Status        string    `json:"status"`
	Online        bool      `json:"online"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	CurrentTasks  []string  `json:"current_tasks"`
	Completed     int64     `json:"completed_count"`
	Failed        int64     `json:"failed_count"`
}

func (s *Server) listWorkers(c *fiber.Ctx) error {
	ws, err := s.d.Store.ListWorkers(c.Context())
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	out := make([]workerView, 0, len(ws))
	for _, w := range ws {
		out = append(out, workerView{
			ID: w.ID, Name: w.Name, Capabilities: w.Capabilities,
			TotalSlots: w.TotalSlots, UsedSlots: w.UsedSlots,
			Status: w.Status, Online: w.Status == "online" || w.Status == "draining",
			LastHeartbeat: w.LastHeartbeat, CurrentTasks: w.CurrentTaskIDs,
			Completed: w.Completed, Failed: w.Failed,
		})
	}
	return c.JSON(fiber.Map{"workers": out})
}

func (s *Server) getWorker(c *fiber.Ctx) error {
	w, err := s.d.Store.GetWorker(c.Context(), c.Params("id"))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if w == nil {
		return c.Status(404).JSON(fiber.Map{"error": "worker not found"})
	}
	// Enrich with per-task detail.
	var tasks []taskView
	for _, tid := range w.CurrentTaskIDs {
		if t, _ := s.d.Store.GetTask(c.Context(), tid); t != nil {
			tasks = append(tasks, toTaskView(t))
		}
	}
	return c.JSON(fiber.Map{
		"worker": workerView{
			ID: w.ID, Name: w.Name, Capabilities: w.Capabilities,
			TotalSlots: w.TotalSlots, UsedSlots: w.UsedSlots,
			Status: w.Status, Online: w.Status == "online" || w.Status == "draining",
			LastHeartbeat: w.LastHeartbeat, CurrentTasks: w.CurrentTaskIDs,
			Completed: w.Completed, Failed: w.Failed,
		},
		"tasks": tasks,
	})
}

func (s *Server) drainWorker(c *fiber.Ctx) error {
	id := c.Params("id")
	if err := s.d.Store.MarkWorkerDraining(c.Context(), id); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"draining": id})
}
