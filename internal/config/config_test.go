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
	if cfg.Runtime.SQLOutputPath != "out.sql" || cfg.Runtime.RunLogPath != "requests.log" {
		t.Fatalf("unexpected runtime config: %+v", cfg.Runtime)
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
	if cfg.Runtime.SQLOutputPath != "res.sql" || cfg.Runtime.RunLogPath != "runlog.log" {
		t.Fatalf("unexpected runtime defaults: %+v", cfg.Runtime)
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
