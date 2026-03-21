package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"api-parser/internal/api"
	_ "api-parser/internal/api/http"
	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "api-parser/internal/db/clickhouse"
	_ "api-parser/internal/db/postgres"
	"api-parser/internal/observability"
	"api-parser/internal/openapi"
)

var (
	loadConfig       = config.Load
	newRequestLogger = func(path string) observability.RequestLogger { return observability.NewFileRequestLogger(path) }
	newAPIConnector  = api.New
	newDBTarget      = db.New
	loadOpenAPI      = openapi.Load
	writeFile        = os.WriteFile
)

func main() {
	configPath := flag.String("config", "conf.yaml", "config path")
	flag.Parse()

	if err := run(context.Background(), *configPath, os.Stdout); err != nil {
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
