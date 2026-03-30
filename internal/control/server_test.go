package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/runner"
)

type staticStateSource struct {
	status runner.Status
	runs   []runner.RunSummary
}

func (s staticStateSource) StatusSnapshot() runner.Status     { return s.status }
func (s staticStateSource) RunsSnapshot() []runner.RunSummary { return s.runs }

func TestServerStatusRunsAndLogsEndpoints(t *testing.T) {
	dir := t.TempDir()
	requestLog := filepath.Join(dir, "requests.log")
	eventLog := filepath.Join(dir, "events.log")
	if err := os.WriteFile(requestLog, []byte("r1\nr2\nr3\n"), 0644); err != nil {
		t.Fatalf("write request log: %v", err)
	}
	if err := os.WriteFile(eventLog, []byte("{\"kind\":\"start\"}\n{\"kind\":\"done\"}\n"), 0644); err != nil {
		t.Fatalf("write event log: %v", err)
	}

	startedAt := time.Date(2026, 3, 24, 8, 0, 0, 0, time.UTC)
	state := staticStateSource{
		status: runner.Status{
			JobMode:               runner.JobModePeriodic,
			Phase:                 runner.PhaseSleeping,
			Cycle:                 3,
			StartedAt:             &startedAt,
			RequestCount:          12,
			FailedRequestCount:    1,
			ManagedTableCount:     2,
			PlannedRowCount:       7,
			AppliedStatementCount: 4,
		},
		runs: []runner.RunSummary{{
			Status: runner.Status{
				JobMode: runner.JobModePeriodic,
				Phase:   runner.PhaseSleeping,
				Cycle:   3,
			},
			Outcome: runner.RunOutcomeSucceeded,
		}},
	}

	srv := NewServer(config.Config{
		Runtime: config.RuntimeConfig{
			RunLogPath:   requestLog,
			EventLogPath: eventLog,
		},
	}, state)
	t.Run("status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
		resp := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(resp, req)
		var body runner.Status
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode status: %v", err)
		}
		if body.Cycle != 3 || body.RequestCount != 12 || body.Phase != runner.PhaseSleeping {
			t.Fatalf("unexpected status body: %+v", body)
		}
	})

	t.Run("runs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/runs", nil)
		resp := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(resp, req)
		var body []runner.RunSummary
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode runs: %v", err)
		}
		if len(body) != 1 || body[0].Outcome != runner.RunOutcomeSucceeded {
			t.Fatalf("unexpected runs body: %+v", body)
		}
	})

	t.Run("logs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/logs?source=requests&tail=2", nil)
		resp := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(resp, req)
		var body struct {
			Lines []string `json:"lines"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode logs: %v", err)
		}
		if len(body.Lines) != 2 || body.Lines[0] != "r2" || body.Lines[1] != "r3" {
			t.Fatalf("unexpected log lines: %+v", body.Lines)
		}
	})

	t.Run("health", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		resp := httptest.NewRecorder()
		srv.httpServer.Handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("unexpected health status: %d", resp.Code)
		}
	})
}
