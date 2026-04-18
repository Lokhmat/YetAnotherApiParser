package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunImageBuildPreparesContextAndInvokesDocker(t *testing.T) {
	repoRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoRoot, "Dockerfile"), []byte("FROM scratch\n"), 0644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoRoot, "README.md"), []byte("repo\n"), 0644); err != nil {
		t.Fatalf("write README: %v", err)
	}

	openapiPath := filepath.Join(t.TempDir(), "api.json")
	configPath := filepath.Join(t.TempDir(), "conf.yaml")
	if err := os.WriteFile(openapiPath, []byte(`{"openapi":"3.0.3"}`), 0644); err != nil {
		t.Fatalf("write openapi: %v", err)
	}
	if err := os.WriteFile(configPath, []byte("openapi_path: ./old.json\nruntime:\n  run_log_path: run.log\n"), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	buildDir := filepath.Join(t.TempDir(), "buildctx")
	restore := stubAPICtlDeps(t)
	defer restore()

	getwd = func() (string, error) { return repoRoot, nil }
	makeTempDir = func() (string, error) { return buildDir, nil }
	removeAll = func(string) error { return nil }

	var dockerArgs []string
	execDocker = func(_ context.Context, _, _ io.Writer, args ...string) error {
		dockerArgs = append([]string{}, args...)
		return nil
	}

	var stdout bytes.Buffer
	if err := run(context.Background(), []string{"image", "build", "--tag", "parser:test", "--openapi", openapiPath, "--config", configPath}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run returned error: %v", err)
	}

	expectedConfigPath := filepath.Join(buildDir, "bundle", "config.yaml")
	data, err := os.ReadFile(expectedConfigPath)
	if err != nil {
		t.Fatalf("read baked config: %v", err)
	}
	if !strings.Contains(string(data), "openapi_path: /app/bundle/api.json") {
		t.Fatalf("expected rewritten openapi path, got %s", string(data))
	}
	if _, err := os.Stat(filepath.Join(buildDir, "bundle", "api.json")); err != nil {
		t.Fatalf("expected bundled openapi file: %v", err)
	}
	if len(dockerArgs) != 4 || dockerArgs[0] != "build" || dockerArgs[1] != "-t" || dockerArgs[2] != "parser:test" || dockerArgs[3] != buildDir {
		t.Fatalf("unexpected docker args: %+v", dockerArgs)
	}
	if !strings.Contains(stdout.String(), "Built image parser:test") {
		t.Fatalf("unexpected stdout: %s", stdout.String())
	}
}

func TestRunStatusRunsAndLogsCommands(t *testing.T) {
	startedAt := time.Date(2026, 3, 24, 9, 0, 0, 0, time.UTC)
	restore := stubAPICtlDeps(t)
	defer restore()
	httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			var payload any
			switch req.URL.Path {
			case "/v1/status":
				payload = map[string]any{
					"job_mode":                "periodic",
					"phase":                   "sleeping",
					"cycle":                   4,
					"started_at":              startedAt,
					"request_count":           10,
					"failed_request_count":    1,
					"managed_table_count":     2,
					"planned_row_count":       8,
					"applied_statement_count": 3,
				}
			case "/v1/runs":
				payload = []map[string]any{{
					"job_mode":                "periodic",
					"phase":                   "sleeping",
					"cycle":                   4,
					"request_count":           10,
					"failed_request_count":    1,
					"managed_table_count":     2,
					"planned_row_count":       8,
					"applied_statement_count": 3,
					"outcome":                 "succeeded",
				}}
			case "/v1/logs":
				payload = map[string]any{"lines": []string{"a", "b"}}
			case "/v1/cycle/trigger":
				if req.Method != http.MethodPost {
					t.Fatalf("expected POST for trigger, got %s", req.Method)
				}
				payload = map[string]any{"status": "scheduled"}
			default:
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       ioutil.NopCloser(strings.NewReader("not found")),
					Header:     make(http.Header),
				}, nil
			}
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal payload: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(bytes.NewReader(data)),
				Header:     make(http.Header),
			}, nil
		}),
	}

	t.Run("status", func(t *testing.T) {
		var out bytes.Buffer
		if err := run(context.Background(), []string{"status", "--addr", "http://parser.local"}, &out, &bytes.Buffer{}); err != nil {
			t.Fatalf("run status returned error: %v", err)
		}
		if !strings.Contains(out.String(), "job_mode=periodic phase=sleeping cycle=4") {
			t.Fatalf("unexpected status output: %s", out.String())
		}
	})

	t.Run("runs", func(t *testing.T) {
		var out bytes.Buffer
		if err := run(context.Background(), []string{"runs", "--addr", "http://parser.local"}, &out, &bytes.Buffer{}); err != nil {
			t.Fatalf("run runs returned error: %v", err)
		}
		if !strings.Contains(out.String(), "cycle=4 outcome=succeeded") {
			t.Fatalf("unexpected runs output: %s", out.String())
		}
	})

	t.Run("logs", func(t *testing.T) {
		var out bytes.Buffer
		if err := run(context.Background(), []string{"logs", "--addr", "http://parser.local", "--source", "events", "--tail", "2"}, &out, &bytes.Buffer{}); err != nil {
			t.Fatalf("run logs returned error: %v", err)
		}
		if strings.TrimSpace(out.String()) != "a\nb" {
			t.Fatalf("unexpected logs output: %q", out.String())
		}
	})

	t.Run("cycle start", func(t *testing.T) {
		var out bytes.Buffer
		if err := run(context.Background(), []string{"cycle", "start", "--addr", "http://parser.local"}, &out, &bytes.Buffer{}); err != nil {
			t.Fatalf("run cycle start returned error: %v", err)
		}
		if strings.TrimSpace(out.String()) != "cycle trigger accepted" {
			t.Fatalf("unexpected cycle output: %q", out.String())
		}
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func stubAPICtlDeps(t *testing.T) func() {
	t.Helper()
	oldExecDocker := execDocker
	oldMakeTempDir := makeTempDir
	oldRemoveAll := removeAll
	oldReadFile := readFile
	oldWriteFile := writeFile
	oldMkdirAll := mkdirAll
	oldHTTPClient := httpClient
	oldGetwd := getwd

	return func() {
		execDocker = oldExecDocker
		makeTempDir = oldMakeTempDir
		removeAll = oldRemoveAll
		readFile = oldReadFile
		writeFile = oldWriteFile
		mkdirAll = oldMkdirAll
		httpClient = oldHTTPClient
		getwd = oldGetwd
	}
}
