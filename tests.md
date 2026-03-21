# Tests Guide

This project uses a risk-based testing strategy. The goal is to prove the critical behavior of each package, not to chase an arbitrary percentage.

## Canonical Command

You can run the full suite with:

```bash
go test ./...
```

## Package Coverage Map

- `cmd`: startup wiring, provider selection, OpenAPI loading, SQL export, DB apply fallback to file
- `internal/config`: YAML parsing, defaults, duration conversion, retry normalization, config load failures
- `internal/api`: provider registry lookup and unknown-provider errors
- `internal/api/http`: request construction, auth redaction in logs, retries, transport and status-code failures
- `internal/core`: dependency resolution, auth parameter behavior, pagination, extraction rules, marked-mode relations
- `internal/db`: provider registry lookup and unknown-provider errors
- `internal/db/postgres`: SQL rendering, deduplication, PK handling
- `internal/db/clickhouse`: SQL rendering, PK and `ORDER BY` behavior, type translation
- `internal/observability`: stable request log formatting, sorted params, error logging, failure-tolerant file writes
- `internal/openapi`: spec load success and file/content failures

## Review Rule

Any new behavior or bug fix should add or update tests in the package that owns that behavior. If the change crosses package boundaries, prefer a focused unit test in the owning package first, and add startup or integration-style coverage only when wiring is the real risk.

## Test Design Notes

- Prefer table-driven tests for parser and config edge cases.
- Keep connector and adapter tests deterministic by using fake transports and in-memory plans instead of live services.
- Use temporary files and directories for config, spec, and log-path tests.
- Keep `cmd` tests focused on orchestration. Do not duplicate `core` logic there.
