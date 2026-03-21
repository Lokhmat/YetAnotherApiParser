package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"api-parser/internal/api"
	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	"api-parser/internal/observability"
)

type cmdFakeConnector struct{}

func (cmdFakeConnector) Fetch(context.Context, core.FetchRequest) (core.FetchResult, error) {
	return core.FetchResult{}, nil
}

type cmdFakeTarget struct {
	exportSQL   []byte
	applyResult core.ApplyResult
	applyErr    error
}

func (t *cmdFakeTarget) Apply(context.Context, *core.MigrationPlan) (core.ApplyResult, error) {
	return t.applyResult, t.applyErr
}

func (t *cmdFakeTarget) ExportSQL(*core.MigrationPlan) ([]byte, error) {
	return t.exportSQL, nil
}

func (t *cmdFakeTarget) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true}
}

func TestRunHappyPath(t *testing.T) {
	specPath := writeCmdSpec(t)
	sqlPath := filepath.Join(t.TempDir(), "res.sql")
	apiName := "test_api_happy"
	dbName := "test_db_happy"
	target := &cmdFakeTarget{
		exportSQL:   []byte("SELECT 1;"),
		applyResult: core.ApplyResult{AppliedCount: 3},
	}

	api.Register(apiName, func(cfg config.APIConfig, logger observability.RequestLogger) (core.APIConnector, error) {
		if cfg.BaseURL != "https://api.example.com" {
			t.Fatalf("unexpected api config: %+v", cfg)
		}
		if logger == nil {
			t.Fatal("expected logger")
		}
		return cmdFakeConnector{}, nil
	})
	db.Register(dbName, func(cfg config.DatabaseConfig) (core.MigrationTarget, error) {
		if cfg.ConnectionString != "memory://db" {
			t.Fatalf("unexpected db config: %+v", cfg)
		}
		return target, nil
	})

	configPath := writeCmdConfig(t, specPath, sqlPath, apiName, dbName)
	var out bytes.Buffer

	if err := run(context.Background(), configPath, &out); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "Base URL: https://api.example.com") {
		t.Fatalf("expected base URL in output, got %s", output)
	}
	if !strings.Contains(output, "Applied 3 migrations") {
		t.Fatalf("expected apply output, got %s", output)
	}
	if !strings.Contains(output, "SELECT 1;") {
		t.Fatalf("expected exported SQL in output, got %s", output)
	}
}

func TestRunWritesSQLWhenApplyFails(t *testing.T) {
	specPath := writeCmdSpec(t)
	sqlPath := filepath.Join(t.TempDir(), "fallback.sql")
	apiName := "test_api_fallback"
	dbName := "test_db_fallback"
	target := &cmdFakeTarget{
		exportSQL: []byte("INSERT INTO events VALUES (1);"),
		applyErr:  errors.New("db unavailable"),
	}

	api.Register(apiName, func(config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return cmdFakeConnector{}, nil
	})
	db.Register(dbName, func(config.DatabaseConfig) (core.MigrationTarget, error) {
		return target, nil
	})

	configPath := writeCmdConfig(t, specPath, sqlPath, apiName, dbName)
	var out bytes.Buffer

	if err := run(context.Background(), configPath, &out); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	data, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Fatalf("read fallback SQL: %v", err)
	}
	if string(data) != "INSERT INTO events VALUES (1);" {
		t.Fatalf("unexpected fallback SQL: %s", data)
	}
	if !strings.Contains(out.String(), "Migrations saved to "+sqlPath) {
		t.Fatalf("expected fallback output, got %s", out.String())
	}
}

func TestRunReturnsProviderSelectionError(t *testing.T) {
	specPath := writeCmdSpec(t)
	configPath := writeCmdConfig(t, specPath, filepath.Join(t.TempDir(), "res.sql"), "missing_api", "postgres")
	var out bytes.Buffer

	err := run(context.Background(), configPath, &out)
	if err == nil || !strings.Contains(err.Error(), `build api provider: unknown api provider "missing_api"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func writeCmdSpec(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "spec.json")
	spec := `{
  "openapi": "3.0.3",
  "info": {"title": "test", "version": "1.0.0"},
  "paths": {}
}`
	if err := os.WriteFile(path, []byte(spec), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return path
}

func writeCmdConfig(t *testing.T, specPath, sqlPath, apiProvider, dbProvider string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "conf.yaml")
	body := fmt.Sprintf(`
openapi_path: %q
api:
  provider: %q
  base_url: "https://api.example.com"
database:
  provider: %q
  connection_string: "memory://db"
runtime:
  sql_output_path: %q
  run_log_path: %q
`, specPath, apiProvider, dbProvider, sqlPath, filepath.Join(t.TempDir(), "run.log"))
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
