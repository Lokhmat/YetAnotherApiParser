package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"api-parser/internal/config"
	"api-parser/internal/database"
	"api-parser/internal/fetcher"
	"api-parser/internal/migration"
	"api-parser/internal/openapi"
)

func main() {
	configPath := flag.String("config", "conf.yaml", "config path")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	spec, err := openapi.Load(context.Background(), cfg.OpenAPIPath)
	if err != nil {
		log.Fatalf("load openapi: %v", err)
	}

	fmt.Printf("Base URL: %s\n", cfg.API.BaseURL)

	// Generate migrations with rate limiting
	maxRPM := cfg.API.MaxRPM
	if maxRPM <= 0 {
		maxRPM = 60 // default to 60 requests per minute
	}
	requestTimeout := time.Duration(cfg.API.RequestTimeout) * time.Second
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	basicRetryTimeout := time.Duration(cfg.API.Retries.BasicRetryTimeout) * time.Second
	if basicRetryTimeout <= 0 {
		basicRetryTimeout = 1 * time.Second
	}
	errorsMaxRetries := cfg.API.Retries.ErrorsMaxRetries
	if errorsMaxRetries < 0 {
		errorsMaxRetries = 0
	}
	mig := migration.New(maxRPM, fetcher.ClientConfig{
		RequestTimeout:    requestTimeout,
		ErrorsMaxRetries:  errorsMaxRetries,
		BasicRetryTimeout: basicRetryTimeout,
	})
	migrations, err := mig.GenerateMigrations(context.Background(), spec, cfg.API.BaseURL)
	if err != nil {
		log.Fatalf("generate migrations: %v", err)
	}

	fmt.Printf("\nGenerated %d migrations:\n", len(migrations))
	for i, ddl := range migrations {
		fmt.Printf("\nMigration %d:\n%s\n", i+1, ddl)
	}

	// Apply migrations to database
	db, err := database.New(cfg.Database.ConnectionString)
	if err != nil {
		log.Printf("database connection failed: %v", err)
		log.Println("Saving migrations to res.sql file...")

		// Write migrations to file
		if len(migrations) > 0 {
			err := os.WriteFile("res.sql", []byte(strings.Join(migrations, "\n\n")), 0644)
			if err != nil {
				log.Fatalf("failed to write migrations to file: %v", err)
			}
			fmt.Println("Migrations saved to res.sql")
		}
		return
	}
	defer db.Close()

	for i, ddl := range migrations {
		if err := db.Exec(ddl); err != nil {
			log.Printf("failed to apply migration %d: %v", i+1, err)
			continue
		}
		fmt.Printf("Applied migration %d\n", i+1)
	}
}
