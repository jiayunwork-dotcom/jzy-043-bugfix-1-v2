package api

import (
	"errors"

	"github.com/asyncflow/engine/internal/domain"
	"github.com/asyncflow/engine/internal/orchestration"
	"github.com/gofiber/fiber/v2"
)

type dagNodeReq struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      any             `json:"payload"`
	Priority     domain.Priority `json:"priority"`
	TimeoutSecs  int             `json:"timeout_seconds"`
	MaxRetries   int             `json:"max_retries"`
	RetryPolicy  *retryPolicyReq `json:"retry_policy"`
	CallbackURL  string          `json:"callback_url"`
	Dependencies []string        `json:"dependencies"`
}

type submitDAGRequest struct {
	Name          string       `json:"name"`
	FailurePolicy string       `json:"failure_policy"` // terminate | skip | retry
	Nodes         []dagNodeReq `json:"nodes"`
}

func (r submitDAGRequest) toDef() (domain.DAGDef, error) {
	def := domain.DAGDef{Name: r.Name, FailurePolicy: domain.DAGFailurePolicy(r.FailurePolicy)}
	if def.FailurePolicy == "" {
		def.FailurePolicy = domain.DAGAbort
	}
	for _, n := range r.Nodes {
		payload, err := jsonMarshal(n.Payload)
		if err != nil {
			return def, err
		}
		ndef := domain.DAGNodeDef{
			ID: n.ID, Type: n.Type, Payload: payload,
			Priority: n.Priority, TimeoutSecs: n.TimeoutSecs,
			MaxRetries: n.MaxRetries, CallbackURL: n.CallbackURL,
			Dependencies: n.Dependencies,
		}
		if ndef.Priority == "" {
			ndef.Priority = domain.PriorityNormal
		}
		if ndef.TimeoutSecs <= 0 {
			ndef.TimeoutSecs = 60
		}
		if n.RetryPolicy != nil {
			ndef.RetryPolicy = domain.RetryPolicy{
				Kind: n.RetryPolicy.Kind, BaseInterval: n.RetryPolicy.BaseInterval,
				MaxRetries: n.RetryPolicy.MaxRetries, CronExpression: n.RetryPolicy.CronExpression,
			}
		} else {
			ndef.RetryPolicy = domain.RetryPolicy{Kind: domain.RetryExponential, BaseInterval: 5, MaxRetries: ndef.MaxRetries}
		}
		def.Nodes = append(def.Nodes, ndef)
	}
	return def, nil
}

func (s *Server) validateDAG(c *fiber.Ctx) error {
	var req submitDAGRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body: " + err.Error()})
	}
	def, err := req.toDef()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	layers, err := orchestration.Validate(def)
	var cycleErr *orchestration.CycleError
	if errors.As(err, &cycleErr) {
		return c.Status(422).JSON(fiber.Map{"valid": false, "cycle": cycleErr.Cycle, "error": err.Error()})
	}
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"valid": false, "error": err.Error()})
	}
	return c.JSON(fiber.Map{"valid": true, "layers": layers})
}

func (s *Server) submitDAG(c *fiber.Ctx) error {
	var req submitDAGRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "invalid body: " + err.Error()})
	}
	def, err := req.toDef()
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	d, err := s.d.DAG.Submit(c.Context(), def)
	if err != nil {
		var cycleErr *orchestration.CycleError
		if errors.As(err, &cycleErr) {
			return c.Status(422).JSON(fiber.Map{"cycle": cycleErr.Cycle, "error": err.Error()})
		}
		return c.Status(400).JSON(fiber.Map{"error": err.Error()})
	}
	return c.Status(201).JSON(fiber.Map{"dag_id": d.ID, "status": string(d.Status)})
}

func dagSummary(d *domain.DAG) fiber.Map {
	return fiber.Map{
		"id": d.ID, "name": d.Name, "failure_policy": string(d.FailurePolicy),
		"status": string(d.Status), "created_at": d.CreatedAt, "finished_at": d.FinishedAt,
		"node_count": len(d.Def.Nodes),
	}
}

func (s *Server) listDAGs(c *fiber.Ctx) error {
	ds, err := s.d.Store.ListDAGs(c.Context(), atoiDefault(c.Query("limit"), 50))
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	out := make([]fiber.Map, 0, len(ds))
	for _, d := range ds {
		out = append(out, dagSummary(d))
	}
	return c.JSON(fiber.Map{"dags": out})
}

func (s *Server) getDAG(c *fiber.Ctx) error {
	id := c.Params("id")
	d, err := s.d.Store.GetDAG(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	if d == nil {
		return c.Status(404).JSON(fiber.Map{"error": "dag not found"})
	}
	nodes, err := s.d.Store.ListDAGNodes(c.Context(), id)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	graph, err := orchestration.BuildGraph(d, nodes)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{
		"dag":    dagSummary(d),
		"layers": graph.Layers,
		"nodes":  graph.Nodes,
	})
}
