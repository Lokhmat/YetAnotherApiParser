package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"api-parser/internal/config"
	"api-parser/internal/core"
)

type stubTarget struct{}

func (stubTarget) Apply(context.Context, *core.MigrationPlan) (core.ApplyResult, error) {
	return core.ApplyResult{}, nil
}

func (stubTarget) ApplyFullSync(context.Context, *core.FullSyncPlan) (core.ApplyResult, error) {
	return core.ApplyResult{}, nil
}

func (stubTarget) ExportSQL(*core.MigrationPlan) ([]byte, error) { return nil, nil }

func (stubTarget) ExportFullSyncSQL(*core.FullSyncPlan) ([]byte, error) { return nil, nil }

func (stubTarget) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true}
}

func TestNewReturnsRegisteredProvider(t *testing.T) {
	name := fmt.Sprintf("test-provider-%s", strings.ToLower(t.Name()))
	expectedCfg := config.DatabaseConfig{ConnectionString: "dsn"}

	Register(name, func(cfg config.DatabaseConfig) (core.MigrationTarget, error) {
		if cfg.ConnectionString != expectedCfg.ConnectionString {
			t.Fatalf("unexpected config passed to factory: %+v", cfg)
		}
		return stubTarget{}, nil
	})

	target, err := New(name, expectedCfg)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if _, ok := target.(stubTarget); !ok {
		t.Fatalf("unexpected target type: %T", target)
	}
}

func TestNewReturnsErrorForUnknownProvider(t *testing.T) {
	_, err := New("missing-provider", config.DatabaseConfig{})
	if err == nil || !strings.Contains(err.Error(), `unknown db provider "missing-provider"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}
