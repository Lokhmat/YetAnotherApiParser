package api

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/observability"
)

type stubConnector struct{}

func (stubConnector) Fetch(context.Context, core.FetchRequest) (core.FetchResult, error) {
	return core.FetchResult{}, nil
}

func TestNewReturnsRegisteredProvider(t *testing.T) {
	name := fmt.Sprintf("test-provider-%s", strings.ToLower(t.Name()))
	expectedCfg := config.APIConfig{BaseURL: "https://api.example.com"}

	Register(name, func(cfg config.APIConfig, logger observability.RequestLogger) (core.APIConnector, error) {
		if cfg.BaseURL != expectedCfg.BaseURL {
			t.Fatalf("unexpected config passed to factory: %+v", cfg)
		}
		if logger == nil {
			t.Fatal("expected logger to be passed to factory")
		}
		return stubConnector{}, nil
	})

	connector, err := New(name, expectedCfg, observability.NopRequestLogger{})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, ok := connector.(stubConnector); !ok {
		t.Fatalf("unexpected connector type: %T", connector)
	}
}

func TestNewReturnsErrorForUnknownProvider(t *testing.T) {
	_, err := New("missing-provider", config.APIConfig{}, observability.NopRequestLogger{})
	if err == nil || !strings.Contains(err.Error(), `unknown api provider "missing-provider"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
