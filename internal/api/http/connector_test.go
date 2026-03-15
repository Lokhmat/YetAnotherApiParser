package http

import (
	"bytes"
	"context"
	"io"
	nethttp "net/http"
	"testing"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
)

type roundTripFunc func(*nethttp.Request) (*nethttp.Response, error)

func (f roundTripFunc) RoundTrip(req *nethttp.Request) (*nethttp.Response, error) {
	return f(req)
}

type collectingLogger struct {
	events []observability.RequestEvent
}

func (l *collectingLogger) LogRequest(event observability.RequestEvent) {
	l.events = append(l.events, event)
}

func TestConnectorBuildsRequestAndLogs(t *testing.T) {
	logger := &collectingLogger{}
	cfg := config.APIConfig{
		MaxRPM:         1000,
		RequestTimeout: time.Second,
		Retries: config.RetryConfig{
			ErrorsMaxRetries:  0,
			BasicRetryTimeout: time.Millisecond,
		},
	}
	client := &nethttp.Client{
		Transport: roundTripFunc(func(req *nethttp.Request) (*nethttp.Response, error) {
			if req.URL.String() != "https://api.example.com/users/42?expand=full" {
				t.Fatalf("unexpected url: %s", req.URL.String())
			}
			if req.Header.Get("X-Token") != "secret" {
				t.Fatalf("missing header")
			}
			return &nethttp.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
				Header:     make(nethttp.Header),
			}, nil
		}),
	}

	conn := newConnector(cfg, logger, client)
	result, err := conn.Fetch(context.Background(), core.FetchRequest{
		Method:      nethttp.MethodGet,
		BaseURL:     "https://api.example.com",
		Path:        "/users/{id}",
		PathParams:  map[string]string{"id": "42"},
		QueryParams: map[string]string{"expand": "full"},
		Headers:     map[string]string{"X-Token": "secret"},
	})
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if result.StatusCode != 200 || len(result.Payload) == 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(logger.events) != 1 || logger.events[0].StatusCode != 200 {
		t.Fatalf("unexpected logged events: %+v", logger.events)
	}
}

func TestConnectorRedactsSensitiveAuthParamsInLogs(t *testing.T) {
	logger := &collectingLogger{}
	cfg := config.APIConfig{
		MaxRPM:         1000,
		RequestTimeout: time.Second,
		Retries: config.RetryConfig{
			ErrorsMaxRetries:  0,
			BasicRetryTimeout: time.Millisecond,
		},
	}
	client := &nethttp.Client{
		Transport: roundTripFunc(func(req *nethttp.Request) (*nethttp.Response, error) {
			return &nethttp.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
				Header:     make(nethttp.Header),
			}, nil
		}),
	}

	conn := newConnector(cfg, logger, client)
	_, err := conn.Fetch(context.Background(), core.FetchRequest{
		Method:           nethttp.MethodGet,
		BaseURL:          "https://api.example.com",
		Path:             "/users",
		QueryParams:      map[string]string{"api_key": "query-secret", "expand": "full"},
		Headers:          map[string]string{"X-Token": "header-secret"},
		SensitiveQuery:   map[string]bool{"api_key": true},
		SensitiveHeaders: map[string]bool{"X-Token": true},
	})
	if err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}

	if len(logger.events) != 1 {
		t.Fatalf("expected one log event, got %d", len(logger.events))
	}
	params := logger.events[0].Params
	if params["api_key"] != "***" {
		t.Fatalf("expected query auth param to be redacted, got %q", params["api_key"])
	}
	if params["X-Token"] != "***" {
		t.Fatalf("expected header auth param to be redacted, got %q", params["X-Token"])
	}
	if params["expand"] != "full" {
		t.Fatalf("expected non-sensitive param to remain visible, got %q", params["expand"])
	}
}

func TestConnectorRetriesOnServerError(t *testing.T) {
	attempts := 0
	cfg := config.APIConfig{
		MaxRPM:         1000,
		RequestTimeout: time.Second,
		Retries: config.RetryConfig{
			ErrorsMaxRetries:  1,
			BasicRetryTimeout: time.Millisecond,
		},
	}
	client := &nethttp.Client{
		Transport: roundTripFunc(func(req *nethttp.Request) (*nethttp.Response, error) {
			attempts++
			status := 500
			body := `fail`
			if attempts == 2 {
				status = 200
				body = `{"ok":true}`
			}
			return &nethttp.Response{
				StatusCode: status,
				Body:       io.NopCloser(bytes.NewBufferString(body)),
				Header:     make(nethttp.Header),
			}, nil
		}),
	}

	conn := newConnector(cfg, observability.NopRequestLogger{}, client)
	if _, err := conn.Fetch(context.Background(), core.FetchRequest{
		Method:  nethttp.MethodGet,
		BaseURL: "https://api.example.com",
		Path:    "/health",
	}); err != nil {
		t.Fatalf("Fetch returned error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}
