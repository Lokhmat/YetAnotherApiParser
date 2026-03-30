package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadParsesExplicitValues(t *testing.T) {
	path := writeTempConfig(t, `
openapi_path: spec.yaml
api:
  provider: custom_api
  base_url: https://api.example.com
  max_rpm: 120
  request_timeout: 45
  retries:
    errors_max_retries: 4
    basic_retry_timeout: 3
database:
  provider: custom_db
  connection_string: postgres://user:pass@localhost:5432/db
runtime:
  sql_output_path: out.sql
  run_log_path: requests.log
  event_log_path: events.log
  full_reload_interval_seconds: 45
control:
  listen_addr: 127.0.0.1:9090
  enabled: false
  history_limit: 7
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.OpenAPIPath != "spec.yaml" {
		t.Fatalf("unexpected OpenAPI path: %q", cfg.OpenAPIPath)
	}
	if cfg.API.Provider != "custom_api" || cfg.Database.Provider != "custom_db" {
		t.Fatalf("unexpected providers: api=%q db=%q", cfg.API.Provider, cfg.Database.Provider)
	}
	if cfg.API.RequestTimeout != 45*time.Second {
		t.Fatalf("unexpected request timeout: %v", cfg.API.RequestTimeout)
	}
	if cfg.API.Retries.BasicRetryTimeout != 3*time.Second {
		t.Fatalf("unexpected retry timeout: %v", cfg.API.Retries.BasicRetryTimeout)
	}
	if cfg.API.Retries.ErrorsMaxRetries != 4 {
		t.Fatalf("unexpected retry count: %d", cfg.API.Retries.ErrorsMaxRetries)
	}
	if cfg.Runtime.SQLOutputPath != "out.sql" || cfg.Runtime.RunLogPath != "requests.log" || cfg.Runtime.EventLogPath != "events.log" {
		t.Fatalf("unexpected runtime config: %+v", cfg.Runtime)
	}
	if !cfg.Runtime.FullReloadEnabled || cfg.Runtime.FullReloadInterval != 45*time.Second {
		t.Fatalf("unexpected full reload config: %+v", cfg.Runtime)
	}
	if cfg.Control.ListenAddr != "127.0.0.1:9090" || cfg.ControlEnabled() || cfg.Control.HistoryLimit != 7 {
		t.Fatalf("unexpected control config: %+v", cfg.Control)
	}
}

func TestLoadAppliesDefaultsAndNormalizesRetryBounds(t *testing.T) {
	path := writeTempConfig(t, `
openapi_path: spec.yaml
api:
  base_url: https://api.example.com
  max_rpm: 0
  request_timeout: 0
  retries:
    errors_max_retries: -5
    basic_retry_timeout: 0
database:
  connection_string: postgres://user:pass@localhost:5432/db
runtime: {}
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.API.Provider != "openapi_http" {
		t.Fatalf("expected default api provider, got %q", cfg.API.Provider)
	}
	if cfg.Database.Provider != "postgres" {
		t.Fatalf("expected default db provider, got %q", cfg.Database.Provider)
	}
	if cfg.API.MaxRPM != 60 {
		t.Fatalf("expected default max RPM, got %d", cfg.API.MaxRPM)
	}
	if cfg.API.RequestTimeout != 30*time.Second {
		t.Fatalf("expected default request timeout, got %v", cfg.API.RequestTimeout)
	}
	if cfg.API.Retries.BasicRetryTimeout != time.Second {
		t.Fatalf("expected default retry timeout, got %v", cfg.API.Retries.BasicRetryTimeout)
	}
	if cfg.API.Retries.ErrorsMaxRetries != 0 {
		t.Fatalf("expected negative retries to clamp to 0, got %d", cfg.API.Retries.ErrorsMaxRetries)
	}
	if cfg.Runtime.SQLOutputPath != "res.sql" || cfg.Runtime.RunLogPath != "runlog.log" || cfg.Runtime.EventLogPath != "events.log" {
		t.Fatalf("unexpected runtime defaults: %+v", cfg.Runtime)
	}
	if cfg.Runtime.FullReloadEnabled {
		t.Fatalf("expected full reload to be disabled by default")
	}
	if cfg.Runtime.FullReloadInterval != 0 {
		t.Fatalf("expected zero full reload interval, got %v", cfg.Runtime.FullReloadInterval)
	}
	if !cfg.ControlEnabled() {
		t.Fatalf("expected control API to be enabled by default")
	}
	if cfg.Control.ListenAddr != ":8080" {
		t.Fatalf("unexpected default listen addr: %q", cfg.Control.ListenAddr)
	}
	if cfg.Control.HistoryLimit != 20 {
		t.Fatalf("unexpected default history limit: %d", cfg.Control.HistoryLimit)
	}
}

func TestLoadExpandsEnvironmentVariablesInStringFields(t *testing.T) {
	t.Setenv("OPENAPI_PATH", "/tmp/spec.json")
	t.Setenv("API_BASE", "https://env.example.com")
	t.Setenv("DB_CONN", "postgres://env")
	t.Setenv("RUN_LOG", "/tmp/run.log")
	t.Setenv("EVENT_LOG", "/tmp/events.log")
	t.Setenv("CONTROL_ADDR", "127.0.0.1:8181")

	path := writeTempConfig(t, `
openapi_path: ${OPENAPI_PATH}
api:
  base_url: ${API_BASE}
database:
  connection_string: ${DB_CONN}
runtime:
  run_log_path: ${RUN_LOG}
  event_log_path: ${EVENT_LOG}
control:
  listen_addr: ${CONTROL_ADDR}
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.OpenAPIPath != "/tmp/spec.json" || cfg.API.BaseURL != "https://env.example.com" || cfg.Database.ConnectionString != "postgres://env" {
		t.Fatalf("expected env-expanded core fields, got %+v", cfg)
	}
	if cfg.Runtime.RunLogPath != "/tmp/run.log" || cfg.Runtime.EventLogPath != "/tmp/events.log" {
		t.Fatalf("expected env-expanded runtime paths, got %+v", cfg.Runtime)
	}
	if cfg.Control.ListenAddr != "127.0.0.1:8181" {
		t.Fatalf("expected env-expanded control addr, got %q", cfg.Control.ListenAddr)
	}
}

func TestLoadMissingFileReturnsError(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("expected missing file error")
	}
}

func TestLoadInvalidYAMLReturnsError(t *testing.T) {
	path := writeTempConfig(t, "openapi_path: [broken")

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid YAML error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "yaml") {
		t.Fatalf("expected YAML error, got %v", err)
	}
}

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(path, []byte(strings.TrimSpace(body)), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
