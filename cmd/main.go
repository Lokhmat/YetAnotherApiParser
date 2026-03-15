package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"api-parser/internal/api"
	_ "api-parser/internal/api/http"
	"api-parser/internal/config"
	"api-parser/internal/core"
	"api-parser/internal/db"
	_ "api-parser/internal/db/postgres"
	"api-parser/internal/observability"
	"api-parser/internal/openapi"
)

func main() {
	configPath := flag.String("config", "conf.yaml", "config path")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger := observability.NewFileRequestLogger(cfg.Runtime.RunLogPath)
	apiConnector, err := api.New(cfg.API.Provider, cfg.API, logger)
	if err != nil {
		log.Fatalf("build api provider: %v", err)
	}
	dbTarget, err := db.New(cfg.Database.Provider, cfg.Database)
	if err != nil {
		log.Fatalf("build db provider: %v", err)
	}

	spec, err := openapi.Load(context.Background(), cfg.OpenAPIPath)
	if err != nil {
		log.Fatalf("load openapi: %v", err)
	}

	fmt.Printf("Base URL: %s\n", cfg.API.BaseURL)

	service := core.NewService(apiConnector)
	plan, err := service.GeneratePlan(context.Background(), spec, cfg.API.BaseURL)
	if err != nil {
		log.Fatalf("generate migration plan: %v", err)
	}

	sqlBytes, err := dbTarget.ExportSQL(plan)
	if err != nil {
		log.Fatalf("export sql: %v", err)
	}

	fmt.Printf("\nGenerated %d operations:\n", len(plan.Operations))
	if len(sqlBytes) > 0 {
		fmt.Printf("\n%s\n", string(sqlBytes))
	}

	result, err := dbTarget.Apply(context.Background(), plan)
	if err != nil {
		log.Printf("database apply failed: %v", err)
		log.Printf("saving migrations to %s...", cfg.Runtime.SQLOutputPath)
		if len(sqlBytes) > 0 {
			if err := os.WriteFile(cfg.Runtime.SQLOutputPath, sqlBytes, 0644); err != nil {
				log.Fatalf("failed to write migrations to file: %v", err)
			}
			fmt.Printf("Migrations saved to %s\n", cfg.Runtime.SQLOutputPath)
		}
		return
	}
	fmt.Printf("Applied %d migrations\n", result.AppliedCount)
}
