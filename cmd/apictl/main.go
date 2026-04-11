package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"api-parser/internal/runner"
	"gopkg.in/yaml.v3"
)

var (
	execDocker = func(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		return cmd.Run()
	}
	makeTempDir = func() (string, error) {
		return os.MkdirTemp("", "apictl-build-*")
	}
	removeAll  = os.RemoveAll
	readFile   = os.ReadFile
	writeFile  = os.WriteFile
	mkdirAll   = os.MkdirAll
	httpClient = &http.Client{Timeout: 5 * time.Second}
	getwd      = os.Getwd
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: apictl <image|run|status|runs|logs|cycle> ...")
	}

	switch args[0] {
	case "image":
		return runImage(ctx, args[1:], stdout, stderr)
	case "run":
		return runContainer(ctx, args[1:], stdout, stderr)
	case "status":
		return runStatus(ctx, args[1:], stdout)
	case "runs":
		return runRuns(ctx, args[1:], stdout)
	case "logs":
		return runLogs(ctx, args[1:], stdout)
	case "cycle":
		return runCycle(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runImage(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "build" {
		return errors.New("usage: apictl image build --tag <tag> --openapi <file> --config <file>")
	}

	fs := flag.NewFlagSet("image build", flag.ContinueOnError)
	fs.SetOutput(stderr)
	tag := fs.String("tag", "", "docker image tag")
	openapiPath := fs.String("openapi", "", "openapi file")
	configPath := fs.String("config", "", "config file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if strings.TrimSpace(*tag) == "" || strings.TrimSpace(*openapiPath) == "" || strings.TrimSpace(*configPath) == "" {
		return errors.New("image build requires --tag, --openapi, and --config")
	}

	repoRoot, err := getwd()
	if err != nil {
		return err
	}
	buildDir, err := makeTempDir()
	if err != nil {
		return err
	}
	defer removeAll(buildDir)

	if err := prepareBuildContext(repoRoot, buildDir, *openapiPath, *configPath); err != nil {
		return err
	}

	if err := execDocker(ctx, stdout, stderr, "build", "-t", *tag, buildDir); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "Built image %s\n", *tag)
	return nil
}

func runContainer(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	image := fs.String("image", "", "docker image")
	name := fs.String("name", "api-parser", "container name")
	port := fs.String("port", "8080", "host port for control API")
	var envs stringList
	fs.Var(&envs, "env", "environment variable passthrough KEY=VALUE")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*image) == "" {
		return errors.New("run requires --image")
	}

	dockerArgs := []string{"run", "-d", "--name", *name, "-p", *port + ":8080"}
	for _, env := range envs {
		dockerArgs = append(dockerArgs, "-e", env)
	}
	dockerArgs = append(dockerArgs, *image)
	return execDocker(ctx, stdout, stderr, dockerArgs...)
}

func runStatus(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	addr := fs.String("addr", "", "control API address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*addr) == "" {
		return errors.New("status requires --addr")
	}
	var status runner.Status
	if err := getJSON(ctx, strings.TrimRight(*addr, "/")+"/v1/status", &status); err != nil {
		return err
	}
	renderStatus(stdout, status)
	return nil
}

func runRuns(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("runs", flag.ContinueOnError)
	addr := fs.String("addr", "", "control API address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*addr) == "" {
		return errors.New("runs requires --addr")
	}
	var runs []runner.RunSummary
	if err := getJSON(ctx, strings.TrimRight(*addr, "/")+"/v1/runs", &runs); err != nil {
		return err
	}
	if len(runs) == 0 {
		_, _ = io.WriteString(stdout, "no runs\n")
		return nil
	}
	for _, run := range runs {
		_, _ = fmt.Fprintf(stdout, "cycle=%d outcome=%s phase=%s requests=%d failed_requests=%d tables=%d rows=%d applied=%d\n",
			run.Cycle, run.Outcome, run.Phase, run.RequestCount, run.FailedRequestCount, run.ManagedTableCount, run.PlannedRowCount, run.AppliedStatementCount)
		if run.LastError != "" {
			_, _ = fmt.Fprintf(stdout, "error=%s\n", run.LastError)
		}
	}
	return nil
}

func runLogs(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	addr := fs.String("addr", "", "control API address")
	source := fs.String("source", "requests", "log source")
	tail := fs.Int("tail", 100, "tail size")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*addr) == "" {
		return errors.New("logs requires --addr")
	}
	var response struct {
		Lines []string `json:"lines"`
	}
	if err := getJSON(ctx, fmt.Sprintf("%s/v1/logs?source=%s&tail=%d", strings.TrimRight(*addr, "/"), *source, *tail), &response); err != nil {
		return err
	}
	for _, line := range response.Lines {
		_, _ = fmt.Fprintln(stdout, line)
	}
	return nil
}

func runCycle(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "start" {
		return errors.New("usage: apictl cycle start --addr <addr>")
	}

	fs := flag.NewFlagSet("cycle start", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", "", "control API address")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if strings.TrimSpace(*addr) == "" {
		return errors.New("cycle start requires --addr")
	}
	if err := postJSON(ctx, strings.TrimRight(*addr, "/")+"/v1/cycle/trigger", nil); err != nil {
		return err
	}
	_, _ = io.WriteString(stdout, "cycle trigger accepted\n")
	return nil
}

func renderStatus(w io.Writer, status runner.Status) {
	_, _ = fmt.Fprintf(w, "job_mode=%s phase=%s cycle=%d\n", status.JobMode, status.Phase, status.Cycle)
	_, _ = fmt.Fprintf(w, "requests=%d failed_requests=%d tables=%d rows=%d applied=%d\n",
		status.RequestCount, status.FailedRequestCount, status.ManagedTableCount, status.PlannedRowCount, status.AppliedStatementCount)
	if status.StartedAt != nil {
		_, _ = fmt.Fprintf(w, "started_at=%s\n", status.StartedAt.Format(time.RFC3339))
	}
	if status.FinishedAt != nil {
		_, _ = fmt.Fprintf(w, "finished_at=%s\n", status.FinishedAt.Format(time.RFC3339))
	}
	if status.NextRunAt != nil {
		_, _ = fmt.Fprintf(w, "next_run_at=%s\n", status.NextRunAt.Format(time.RFC3339))
	}
	if status.LastSuccessAt != nil {
		_, _ = fmt.Fprintf(w, "last_success_at=%s\n", status.LastSuccessAt.Format(time.RFC3339))
	}
	if status.LastError != "" {
		_, _ = fmt.Fprintf(w, "last_error=%s\n", status.LastError)
	}
}

func getJSON(ctx context.Context, rawURL string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, normalizeAddr(rawURL), nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func postJSON(ctx context.Context, rawURL string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, normalizeAddr(rawURL), nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if dest == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func normalizeAddr(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	return "http://" + strings.TrimPrefix(raw, "/")
}

func prepareBuildContext(repoRoot, buildDir, openapiPath, configPath string) error {
	if err := copyRepoTree(repoRoot, buildDir); err != nil {
		return err
	}
	bundleDir := filepath.Join(buildDir, "bundle")
	if err := mkdirAll(bundleDir, 0755); err != nil {
		return err
	}
	bakedOpenAPIName := filepath.Base(openapiPath)
	bakedOpenAPIPath := filepath.Join(bundleDir, bakedOpenAPIName)
	if err := copyFile(openapiPath, bakedOpenAPIPath); err != nil {
		return err
	}
	bakedConfig, err := rewriteConfigForBundle(configPath, "/app/bundle/"+bakedOpenAPIName)
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(bundleDir, "config.yaml"), bakedConfig, 0644)
}

func copyRepoTree(repoRoot, buildDir string) error {
	return filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if shouldSkip(rel) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(buildDir, rel)
		if d.IsDir() {
			return mkdirAll(target, 0755)
		}
		return copyFile(path, target)
	})
}

func shouldSkip(rel string) bool {
	parts := strings.Split(rel, string(filepath.Separator))
	for _, part := range parts {
		if part == ".git" || part == ".gocache" {
			return true
		}
	}
	return false
}

func copyFile(src, dst string) error {
	data, err := readFile(src)
	if err != nil {
		return err
	}
	if err := mkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	return writeFile(dst, data, 0644)
}

func rewriteConfigForBundle(configPath, bakedOpenAPIPath string) ([]byte, error) {
	data, err := readFile(configPath)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	doc["openapi_path"] = bakedOpenAPIPath
	return yaml.Marshal(doc)
}

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}
