# Tests Guide

This project uses risk-based coverage. The goal is to prove the parser's fetch, extraction, and database-reconciliation behavior where failures would corrupt output or drift DB state from the source API.

## Canonical Command

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

## Coverage Map

- `cmd`: startup wiring, one-shot execution, periodic full-reload loop, provider capability gating
- `internal/config`: YAML parsing, defaults, duration conversion, retry normalization, config load failures, periodic reload settings
- `internal/api`: provider registry lookup and unknown-provider errors
- `internal/api/http`: request construction, retries, status/error handling, auth redaction
- `internal/core`: dependency resolution, auth parameter behavior, pagination, extraction rules, marked-mode relations, full-sync desired-state conversion, PK validation for periodic reload
- `internal/db`: provider registry lookup and unknown-provider errors
- `internal/db/postgres`: SQL rendering, insert deduplication, PK handling, full-sync upsert/delete reconciliation SQL
- `internal/db/clickhouse`: SQL rendering, PK and `ORDER BY` behavior, type translation, full-sync truncate-and-reload SQL
- `internal/observability`: stable request log formatting, sorted params, error logging, failure-tolerant file writes
- `internal/openapi`: spec load success and file/content failures

## Review Rule

Any feature or bug fix should add or update tests in the package that owns that behavior. If the change crosses package boundaries, prefer a focused unit test in the owning package first, and add startup or integration-style coverage only when wiring is the real risk.

## Current High-Risk Scenarios

- periodic full reload remains disabled by default
- periodic full reload rejects specs that cannot reconcile rows by PK
- full-sync desired state preserves row identity for classic and marked-mode tables
- Postgres full-sync SQL performs upsert for changed rows and delete for missing rows
- ClickHouse full-sync SQL truncates managed tables and reinserts the current snapshot
- one-shot mode remains unchanged when periodic reload is not configured
