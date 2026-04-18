package control

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"api-parser/internal/config"
	"api-parser/internal/runner"
)

type StateSource interface {
	StatusSnapshot() runner.Status
	RunsSnapshot() []runner.RunSummary
	TriggerCycle() error
}

type Server struct {
	httpServer *http.Server
	listener   net.Listener
	cfg        config.Config
	source     StateSource
}

func NewServer(cfg config.Config, source StateSource) *Server {
	s := &Server{
		cfg:    cfg,
		source: source,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/v1/status", s.handleStatus)
	mux.HandleFunc("/v1/runs", s.handleRuns)
	mux.HandleFunc("/v1/cycle/trigger", s.handleTriggerCycle)
	mux.HandleFunc("/v1/logs", s.handleLogs)
	s.httpServer = &http.Server{Handler: mux}
	return s
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.cfg.Control.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = err
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.source.StatusSnapshot())
}

func (s *Server) handleRuns(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.source.RunsSnapshot())
}

func (s *Server) handleTriggerCycle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method must be POST"})
		return
	}
	if err := s.source.TriggerCycle(); err != nil {
		statusCode := http.StatusConflict
		if errors.Is(err, runner.ErrManualCycleUnsupported) {
			statusCode = http.StatusBadRequest
		}
		writeJSON(w, statusCode, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "scheduled"})
}

func (s *Server) handleLogs(w http.ResponseWriter, req *http.Request) {
	source := req.URL.Query().Get("source")
	if source == "" {
		source = "requests"
	}
	path := s.cfg.Runtime.RunLogPath
	switch source {
	case "requests":
		path = s.cfg.Runtime.RunLogPath
	case "events":
		path = s.cfg.Runtime.EventLogPath
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "source must be requests or events"})
		return
	}

	tailCount := 100
	if rawTail := req.URL.Query().Get("tail"); rawTail != "" {
		parsed, err := strconv.Atoi(rawTail)
		if err != nil || parsed <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tail must be a positive integer"})
			return
		}
		tailCount = parsed
	}

	lines, err := tailLines(path, tailCount)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"source": source,
		"lines":  lines,
	})
}

func tailLines(path string, tailCount int) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, err
	}
	text := strings.TrimRight(string(data), "\n")
	if text == "" {
		return []string{}, nil
	}
	lines := strings.Split(text, "\n")
	if tailCount >= len(lines) {
		return lines, nil
	}
	return lines[len(lines)-tailCount:], nil
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
