# Tests Guide

This project uses risk-based coverage. The goal is to prove the parser's fetch, extraction, and database-reconciliation behavior where failures would corrupt output or drift DB state from the source API.

## Canonical Command

```bash
GOCACHE=$(pwd)/.gocache go test ./...
```

## Coverage Map

- `cmd`: startup wiring, one-shot execution, periodic full-reload loop, provider capability gating, control-server startup wiring
- `cmd/apictl`: Docker build/run command assembly, control API client rendering for status/runs/logs
- `internal/config`: YAML parsing, defaults, duration conversion, retry normalization, config load failures, periodic reload settings, control config defaults, env expansion
- `internal/control`: HTTP handlers for health, status, recent runs, and request/event log tails
- `internal/api`: provider registry lookup and unknown-provider errors
- `internal/api/http`: request construction, retries, status/error handling, auth redaction
- `internal/core`: dependency resolution, auth parameter behavior, pagination, extraction rules, marked-mode relations, full-sync desired-state conversion, PK validation for periodic reload
- `internal/db`: provider registry lookup and unknown-provider errors
- `internal/db/postgres`: SQL rendering, insert deduplication, PK handling, full-sync upsert/delete reconciliation SQL
- `internal/db/clickhouse`: SQL rendering, PK and `ORDER BY` behavior, type translation, full-sync truncate-and-reload SQL
- `internal/observability`: stable request log formatting, sorted params, event-log JSON lines, failure-tolerant file writes
- `internal/openapi`: spec load success and file/content failures
- `internal/runner`: one-shot/periodic phase transitions, request/error counters, derived table/row counts, next-run scheduling, bounded run history

## Review Rule

Any feature or bug fix should add or update tests in the package that owns that behavior. If the change crosses package boundaries, prefer a focused unit test in the owning package first, and add startup or integration-style coverage only when wiring is the real risk.

## Current High-Risk Scenarios

- periodic full reload remains disabled by default
- periodic full reload rejects specs that cannot reconcile rows by PK
- full-sync desired state preserves row identity for classic and marked-mode tables
- Postgres full-sync SQL performs upsert for changed rows and delete for missing rows
- ClickHouse full-sync SQL truncates managed tables and reinserts the current snapshot
- one-shot mode remains unchanged when periodic reload is not configured
- control API defaults remain backward compatible for old config files
- string config values expand environment variables so baked Docker configs can defer secrets to runtime
- request counters and failed-request counters reflect actual connector events
- status-derived managed-table and planned-row counts match generated plans
- recent run history is bounded and newest-first
- `apictl image build` rewrites `openapi_path` to the bundled file inside the image context
