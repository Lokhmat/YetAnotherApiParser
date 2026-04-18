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
	"time"

	"api-parser/internal/config"
	"api-parser/internal/control"
	"api-parser/internal/core"
	"api-parser/internal/observability"
)

type cmdFakeConnector struct {
	payload []byte
}

func (c cmdFakeConnector) Fetch(context.Context, core.FetchRequest) (core.FetchResult, error) {
	return core.FetchResult{Payload: c.payload, StatusCode: 200}, nil
}

type cmdFakeTarget struct {
	exportSQL         []byte
	exportFullSyncSQL []byte
	applyResult       core.ApplyResult
	applyFullSync     core.ApplyResult
	applyErr          error
	applyFullSyncErr  error
}

func (t *cmdFakeTarget) Apply(context.Context, *core.MigrationPlan) (core.ApplyResult, error) {
	return t.applyResult, t.applyErr
}

func (t *cmdFakeTarget) ApplyFullSync(context.Context, *core.FullSyncPlan) (core.ApplyResult, error) {
	return t.applyFullSync, t.applyFullSyncErr
}

func (t *cmdFakeTarget) ExportSQL(*core.MigrationPlan) ([]byte, error) {
	return t.exportSQL, nil
}

func (t *cmdFakeTarget) ExportFullSyncSQL(*core.FullSyncPlan) ([]byte, error) {
	return t.exportFullSyncSQL, nil
}

func (t *cmdFakeTarget) LoadCheckpoint(context.Context, string) (*core.Checkpoint, error) {
	return nil, nil
}

func (t *cmdFakeTarget) SaveCheckpoints(context.Context, []core.Checkpoint) error {
	return nil
}

func (t *cmdFakeTarget) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true, CanFullSync: true}
}

type cmdNoFullSyncTarget struct{}

type noopControlServer struct{}

func (noopControlServer) Shutdown(context.Context) error { return nil }

func (cmdNoFullSyncTarget) Apply(context.Context, *core.MigrationPlan) (core.ApplyResult, error) {
	return core.ApplyResult{}, nil
}

func (cmdNoFullSyncTarget) ApplyFullSync(context.Context, *core.FullSyncPlan) (core.ApplyResult, error) {
	return core.ApplyResult{}, nil
}

func (cmdNoFullSyncTarget) ExportSQL(*core.MigrationPlan) ([]byte, error) {
	return nil, nil
}

func (cmdNoFullSyncTarget) ExportFullSyncSQL(*core.FullSyncPlan) ([]byte, error) {
	return nil, nil
}

func (cmdNoFullSyncTarget) LoadCheckpoint(context.Context, string) (*core.Checkpoint, error) {
	return nil, nil
}

func (cmdNoFullSyncTarget) SaveCheckpoints(context.Context, []core.Checkpoint) error {
	return nil
}

func (cmdNoFullSyncTarget) Capabilities() core.Capabilities {
	return core.Capabilities{CanExportSQL: true, CanFullSync: false}
}

func TestRunOneShotHappyPath(t *testing.T) {
	restore := stubMainDeps(t)
	defer restore()

	specPath := writeCmdSpec(t, true)
	configPath := writeCmdConfig(t, specPath, 0, "postgres")
	target := &cmdFakeTarget{
		exportSQL:   []byte("SELECT 1;"),
		applyResult: core.ApplyResult{AppliedCount: 3},
	}
	newAPIConnector = func(string, config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return cmdFakeConnector{payload: []byte(`[{"id":1}]`)}, nil
	}
	newDBTarget = func(string, config.DatabaseConfig) (core.MigrationTarget, error) {
		return target, nil
	}

	var out bytes.Buffer
	if err := run(context.Background(), configPath, &out); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	output := out.String()
	if !strings.Contains(output, "Base URL: https://api.example.com") {
		t.Fatalf("expected base URL in output, got %s", output)
	}
	if !strings.Contains(output, "Applied 3 statements") {
		t.Fatalf("unexpected output: %s", output)
	}
	if !strings.Contains(output, "SELECT 1;") {
		t.Fatalf("expected exported SQL, got %s", output)
	}
}

func TestRunWritesSQLWhenApplyFails(t *testing.T) {
	restore := stubMainDeps(t)
	defer restore()

	specPath := writeCmdSpec(t, true)
	sqlPath := filepath.Join(t.TempDir(), "fallback.sql")
	configPath := writeCmdConfigWithSQLPath(t, specPath, 0, "postgres", sqlPath)
	target := &cmdFakeTarget{
		exportSQL: []byte("INSERT INTO events VALUES (1);"),
		applyErr:  errors.New("db unavailable"),
	}
	newAPIConnector = func(string, config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return cmdFakeConnector{payload: []byte(`[{"id":1}]`)}, nil
	}
	newDBTarget = func(string, config.DatabaseConfig) (core.MigrationTarget, error) {
		return target, nil
	}

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
	restore := stubMainDeps(t)
	defer restore()

	specPath := writeCmdSpec(t, true)
	configPath := writeCmdConfig(t, specPath, 0, "postgres")
	newAPIConnector = func(string, config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return nil, fmt.Errorf(`unknown api provider "missing_api"`)
	}

	err := run(context.Background(), configPath, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `build api provider: unknown api provider "missing_api"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunPeriodicFullReloadLoopsUntilCanceled(t *testing.T) {
	restore := stubMainDeps(t)
	defer restore()

	specPath := writeCmdSpecWithResType(t, true, "full-reload")
	configPath := writeCmdConfig(t, specPath, 5, "postgres")
	target := &cmdFakeTarget{
		exportFullSyncSQL: []byte("DELETE FROM items;"),
		applyFullSync:     core.ApplyResult{AppliedCount: 4},
	}
	newAPIConnector = func(string, config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return cmdFakeConnector{payload: []byte(`[{"id":1}]`)}, nil
	}
	newDBTarget = func(string, config.DatabaseConfig) (core.MigrationTarget, error) {
		return target, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	sleepWithContext = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}

	var out bytes.Buffer
	if err := run(ctx, configPath, &out); err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if !strings.Contains(out.String(), "Cycle 1 applied 4 statements") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestRunRejectsPeriodicReloadWhenProviderLacksFullSync(t *testing.T) {
	restore := stubMainDeps(t)
	defer restore()

	specPath := writeCmdSpecWithResType(t, true, "full-reload")
	configPath := writeCmdConfig(t, specPath, 5, "sqlite")
	newAPIConnector = func(string, config.APIConfig, observability.RequestLogger) (core.APIConnector, error) {
		return cmdFakeConnector{payload: []byte(`[{"id":1}]`)}, nil
	}
	newDBTarget = func(string, config.DatabaseConfig) (core.MigrationTarget, error) {
		return cmdNoFullSyncTarget{}, nil
	}

	err := run(context.Background(), configPath, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "does not support full-reload operations") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func stubMainDeps(t *testing.T) func() {
	t.Helper()

	oldLoadConfig := loadConfig
	oldNewRequestLogger := newRequestLogger
	oldNewEventLogger := newEventLogger
	oldNewRequestTracker := newRequestTracker
	oldNewAPIConnector := newAPIConnector
	oldNewDBTarget := newDBTarget
	oldLoadOpenAPI := loadOpenAPI
	oldWriteFile := writeFile
	oldNewRunner := newRunner
	oldNewControlServer := newControlServer
	oldSleepWithContext := sleepWithContext

	newControlServer = func(config.Config, control.StateSource) (controlServer, error) {
		return noopControlServer{}, nil
	}
	newEventLogger = func(string) observability.EventLogger { return observability.NopEventLogger{} }

	return func() {
		loadConfig = oldLoadConfig
		newRequestLogger = oldNewRequestLogger
		newEventLogger = oldNewEventLogger
		newRequestTracker = oldNewRequestTracker
		newAPIConnector = oldNewAPIConnector
		newDBTarget = oldNewDBTarget
		loadOpenAPI = oldLoadOpenAPI
		writeFile = oldWriteFile
		newRunner = oldNewRunner
		newControlServer = oldNewControlServer
		sleepWithContext = oldSleepWithContext
	}
}

func writeCmdSpec(t *testing.T, withFetchablePath bool) string {
	return writeCmdSpecWithResType(t, withFetchablePath, "one-shot")
}

func writeCmdSpecWithResType(t *testing.T, withFetchablePath bool, resType string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "spec.json")
	spec := fmt.Sprintf(`{
  "openapi": "3.0.3",
  "info": {"title": "test", "version": "1.0.0"},
  "paths": {
    "/items": {
      "get": {
        "x-res-type": %q,
        "responses": {
          "200": {
            "description": "ok",
            "content": {
              "application/json": {
                "schema": {
                  "type": "array",
                  "items": {
                    "type": "object",
                    "properties": {
                      "id": {"type": "integer", "x-pk": true}
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}`, resType)
	if !withFetchablePath {
		spec = `{"openapi":"3.0.3","info":{"title":"test","version":"1.0.0"},"paths":{}}`
	}
	if err := os.WriteFile(path, []byte(spec), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return path
}

func writeCmdConfig(t *testing.T, specPath string, fullReloadInterval int, dbProvider string) string {
	t.Helper()
	return writeCmdConfigWithSQLPath(t, specPath, fullReloadInterval, dbProvider, filepath.Join(t.TempDir(), "res.sql"))
}

func writeCmdConfigWithSQLPath(t *testing.T, specPath string, fullReloadInterval int, dbProvider, sqlPath string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "conf.yaml")
	body := fmt.Sprintf(`
openapi_path: %q
api:
  provider: "test_api"
  base_url: "https://api.example.com"
database:
  provider: %q
  connection_string: "memory://db"
runtime:
  sql_output_path: %q
  run_log_path: %q
  cycle_interval_seconds: %d
`, specPath, dbProvider, sqlPath, filepath.Join(t.TempDir(), "run.log"), fullReloadInterval)
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
