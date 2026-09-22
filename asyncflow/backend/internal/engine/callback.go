package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/asyncflow/engine/internal/domain"
)

// HTTPCallbackPoster POSTs a JSON completion notice to the task callback URL.
// Failures are ignored (best-effort webhook) and never block task completion.
type HTTPCallbackPoster struct {
	client *http.Client
}

func NewCallbackPoster() *HTTPCallbackPoster {
	return &HTTPCallbackPoster{client: &http.Client{Timeout: 10 * time.Second}}
}

type callbackPayload struct {
	TaskID   string `json:"task_id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	Attempts int    `json:"attempts"`
	Error    string `json:"error,omitempty"`
}

func (p *HTTPCallbackPoster) Post(ctx context.Context, url string, t *domain.Task, success bool, errMsg string) {
	if url == "" {
		return
	}
	status := "succeeded"
	if !success {
		status = "failed"
	}
	body, err := json.Marshal(callbackPayload{
		TaskID: t.ID, Type: t.Type, Status: status,
		Attempts: t.Attempts, Error: errMsg,
	})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}
