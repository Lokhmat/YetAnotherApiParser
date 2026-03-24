package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"api-parser/internal/api"
	_ "api-parser/internal/api/http"
	"api-parser/internal/config"
	"api-parser/internal/control"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "api-parser/internal/db/clickhouse"
	_ "api-parser/internal/db/postgres"
	"api-parser/internal/observability"
	"api-parser/internal/openapi"
	"api-parser/internal/runner"
	"github.com/getkin/kin-openapi/openapi3"
)

var (
	loadConfig        = config.Load
	newRequestLogger  = func(path string) observability.RequestLogger { return observability.NewFileRequestLogger(path) }
	newEventLogger    = func(path string) observability.EventLogger { return observability.NewFileEventLogger(path) }
	newRequestTracker = func() *runner.RequestTracker { return runner.NewRequestTracker() }
	newAPIConnector   = api.New
	newDBTarget       = db.New
	loadOpenAPI       = openapi.Load
	writeFile         = os.WriteFile
	newRunner         = func(cfg config.Config, spec *openapi3.T, planner runner.Planner, target core.MigrationTarget, out io.Writer, tracker *runner.RequestTracker, eventLogger observability.EventLogger, writeFile func(string, []byte, fs.FileMode) error, sleep func(context.Context, time.Duration) error) *runner.Runner {
		return runner.New(cfg, spec, planner, target, out, tracker, eventLogger, writeFile, sleep)
	}
	newControlServer = func(cfg config.Config, source control.StateSource) (controlServer, error) {
		server := control.NewServer(cfg, source)
		return server, server.Start()
	}
	sleepWithContext = func(ctx context.Context, delay time.Duration) error {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
)

type controlServer interface {
	Shutdown(ctx context.Context) error
}

func main() {
	configPath := flag.String("config", "conf.yaml", "config path")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *configPath, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, configPath string, out io.Writer) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	requestTracker := newRequestTracker()
	logger := observability.MultiRequestLogger{
		newRequestLogger(cfg.Runtime.RunLogPath),
		requestTracker,
	}
	eventLogger := newEventLogger(cfg.Runtime.EventLogPath)
	apiConnector, err := newAPIConnector(cfg.API.Provider, cfg.API, logger)
	if err != nil {
		return fmt.Errorf("build api provider: %w", err)
	}
	dbTarget, err := newDBTarget(cfg.Database.Provider, cfg.Database)
	if err != nil {
		return fmt.Errorf("build db provider: %w", err)
	}

	spec, err := loadOpenAPI(ctx, cfg.OpenAPIPath)
	if err != nil {
		return fmt.Errorf("load openapi: %w", err)
	}

	fmt.Fprintf(out, "Base URL: %s\n", cfg.API.BaseURL)

	service := core.NewService(apiConnector)
	runLoop := newRunner(cfg, spec, service, dbTarget, out, requestTracker, eventLogger, writeFile, sleepWithContext)

	var srv controlServer
	if cfg.ControlEnabled() {
		srv, err = newControlServer(cfg, runLoop)
		if err != nil {
			return fmt.Errorf("start control server: %w", err)
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdownCtx)
		}()
	}

	return runLoop.Run(ctx)
}
