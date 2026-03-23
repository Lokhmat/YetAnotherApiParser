package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"api-parser/internal/api"
	_ "api-parser/internal/api/http"
	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "api-parser/internal/db/clickhouse"
	_ "api-parser/internal/db/postgres"
	"api-parser/internal/observability"
	"api-parser/internal/openapi"
	"github.com/getkin/kin-openapi/openapi3"
)

var (
	loadConfig       = config.Load
	newRequestLogger = func(path string) observability.RequestLogger { return observability.NewFileRequestLogger(path) }
	newAPIConnector  = api.New
	newDBTarget      = db.New
	loadOpenAPI      = openapi.Load
	writeFile        = os.WriteFile
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

	logger := newRequestLogger(cfg.Runtime.RunLogPath)
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
	if !cfg.Runtime.FullReloadEnabled {
		return runOneShot(ctx, out, cfg, spec, service, dbTarget)
	}

	if !dbTarget.Capabilities().CanFullSync {
		return fmt.Errorf("db provider %q does not support periodic full reload", cfg.Database.Provider)
	}

	fmt.Fprintf(out, "Periodic full reload enabled: every %ds\n", cfg.Runtime.FullReloadIntervalSeconds)

	for cycle := 1; ; cycle++ {
		fullSyncPlan, err := service.GenerateFullSyncPlan(ctx, spec, cfg.API.BaseURL)
		if err != nil {
			return fmt.Errorf("generate full sync plan: %w", err)
		}

		sqlBytes, err := dbTarget.ExportFullSyncSQL(fullSyncPlan)
		if err != nil {
			return fmt.Errorf("export full sync sql: %w", err)
		}
		result, err := dbTarget.ApplyFullSync(ctx, fullSyncPlan)
		if err != nil {
			log.Printf("periodic full reload cycle %d failed: %v", cycle, err)
			if len(sqlBytes) > 0 {
				log.Printf("saving full sync SQL to %s...", cfg.Runtime.SQLOutputPath)
				if writeErr := writeFile(cfg.Runtime.SQLOutputPath, sqlBytes, 0644); writeErr != nil {
					return fmt.Errorf("failed to write migrations to file: %w", writeErr)
				}
				fmt.Fprintf(out, "Migrations saved to %s\n", cfg.Runtime.SQLOutputPath)
			}
		} else {
			fmt.Fprintf(out, "Full reload cycle %d applied %d statements\n", cycle, result.AppliedCount)
		}

		if err := sleepWithContext(ctx, cfg.Runtime.FullReloadInterval); err != nil {
			if err == context.Canceled {
				return nil
			}
			return err
		}
	}
}

func runOneShot(ctx context.Context, out io.Writer, cfg config.Config, spec *openapi3.T, service *core.Service, dbTarget core.MigrationTarget) error {
	plan, err := service.GeneratePlan(ctx, spec, cfg.API.BaseURL)
	if err != nil {
		return fmt.Errorf("generate migration plan: %w", err)
	}

	sqlBytes, err := dbTarget.ExportSQL(plan)
	if err != nil {
		return fmt.Errorf("export sql: %w", err)
	}

	fmt.Fprintf(out, "\nGenerated %d operations:\n", len(plan.Operations))
	if len(sqlBytes) > 0 {
		fmt.Fprintf(out, "\n%s\n", string(sqlBytes))
	}

	result, err := dbTarget.Apply(ctx, plan)
	if err != nil {
		log.Printf("database apply failed: %v", err)
		log.Printf("saving migrations to %s...", cfg.Runtime.SQLOutputPath)
		if len(sqlBytes) > 0 {
			if err := writeFile(cfg.Runtime.SQLOutputPath, sqlBytes, 0644); err != nil {
				return fmt.Errorf("failed to write migrations to file: %w", err)
			}
			fmt.Fprintf(out, "Migrations saved to %s\n", cfg.Runtime.SQLOutputPath)
		}
		return nil
	}
	fmt.Fprintf(out, "Applied %d migrations\n", result.AppliedCount)
	return nil
}
