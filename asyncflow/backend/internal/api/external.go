package api

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
)

type registerWorkerReq struct {
	WorkerID     string   `json:"worker_id"`
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities"`
	Slots        int      `json:"slots"`
}

func (s *Server) registerExternalWorker(c *fiber.Ctx) error {
	var req registerWorkerReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if req.WorkerID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "worker_id required"})
	}
	if req.Slots <= 0 {
		req.Slots = 1
	}
	if len(req.Capabilities) == 0 {
		req.Capabilities = []string{"*"}
	}
	w, err := s.d.ExtMgr.Register(c.Context(), req.WorkerID, req.Name, req.Capabilities, req.Slots)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(201).JSON(fiber.Map{"worker_id": w.ID(), "status": "registered"})
}

type heartbeatReq struct {
	WorkerID     string   `json:"worker_id"`
	CurrentTasks []string `json:"current_tasks"`
}

func (s *Server) externalHeartbeat(c *fiber.Ctx) error {
	var req heartbeatReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	if err := s.d.ExtMgr.Heartbeat(c.Context(), req.WorkerID, req.CurrentTasks); err != nil {
		return c.Status(404).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"ok": true})
}

func (s *Server) claimTask(c *fiber.Ctx) error {
	id := c.Params("id")
	w := s.d.ExtMgr.Get(id)
	if w == nil {
		return c.Status(404).JSON(fiber.Map{"error": "worker not registered"})
	}
	wait := time.Duration(atoiDefault(c.Query("wait_ms", "25000"), 25000)) * time.Millisecond
	if wait > 60*time.Second {
		wait = 60 * time.Second
	}
	t := w.PollClaim(c.Context(), wait)
	if t == nil {
		return c.SendStatus(204)
	}
	s.d.ExtMgr.TaskStarted(id, t)
	_ = s.d.ExtMgr.Heartbeat(c.Context(), id, s.d.ExtMgr.SnapshotIDs(id))
	return c.JSON(fiber.Map{"task": toTaskView(t)})
}

type completeReq struct {
	TaskID   string          `json:"task_id"`
	Success  bool            `json:"success"`
	Error    string          `json:"error"`
	Category string          `json:"category"`
	Result   json.RawMessage `json:"result"`
}

func (s *Server) completeTask(c *fiber.Ctx) error {
	workerID := c.Params("id")
	var req completeReq
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body"})
	}
	t, err := s.d.Store.GetTask(c.Context(), req.TaskID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if t == nil {
		return c.Status(404).JSON(fiber.Map{"error": "task not found"})
	}
	t.WorkerID = workerID
	s.d.Engine.ReportResult(c.Context(), t, req.Success, []byte(req.Result), req.Error, req.Category)
	s.d.ExtMgr.TaskFinished(workerID, req.TaskID)
	w := s.d.ExtMgr.Get(workerID)
	if w != nil {
		w.ReleaseSlot(req.TaskID)
	}
	_ = s.d.Store.IncWorkerStats(c.Context(), workerID, req.Success, !req.Success)
	_ = s.d.ExtMgr.Heartbeat(c.Context(), workerID, s.d.ExtMgr.SnapshotIDs(workerID))
	return c.JSON(fiber.Map{"ok": true})
}
