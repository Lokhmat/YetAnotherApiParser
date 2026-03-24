# API Parser

`api-parser` reads an OpenAPI spec, fetches selected API endpoints, builds a database-agnostic migration plan, and executes it through a configured database adapter.

Current built-in providers:

- API provider: `openapi_http`
- DB providers: `postgres`, `clickhouse`

With the built-in adapters it generates provider-specific SQL such as:

- `CREATE TABLE ...`
- `INSERT INTO ...`

It can:

- resolve endpoint dependencies with `x-param-data` + `x-response-data`
- inject env-backed auth parameters with `x-auth`
- fetch in dependency order
- flatten marked nested response objects into separate tables (`x-table-name`)
- export SQL through the DB adapter
- apply migrations directly to DB
- periodically refetch the full API snapshot and reconcile managed DB tables
- write request run logs to a configurable path
- write structured runtime event logs to a configurable path
- expose a small internal control API for status, recent runs, and log tailing
- be packaged into a job-specific Docker image with the bundled `apictl` operator CLI

## Requirements

- Go 1.24+
- Docker (for `apictl image build` / `apictl run`)
- PostgreSQL or ClickHouse (optional; if DB is unavailable, SQL is still written to file)

## Configuration

Create `conf.yaml`:

```yaml
openapi_path: "example/api.json"

runtime:
  sql_output_path: "res.sql"
  run_log_path: "runlog.log"
  event_log_path: "events.log"
  full_reload_interval_seconds: 0

control:
  listen_addr: ":8080"
  enabled: true
  history_limit: 20

api:
  provider: "openapi_http"
  base_url: "https://api.example.com"
  max_rpm: 60
  request_timeout: 30  # seconds
  retries:
    errors_max_retries: 3
    basic_retry_timeout: 2  # seconds

database:
  provider: "postgres"
  connection_string: "host=localhost port=5440 user=postgres password=postgres dbname=api_parser"
```

ClickHouse example:

```yaml
database:
  provider: "clickhouse"
  connection_string: "<clickhouse-go database/sql DSN>"
```

Use the official `clickhouse-go` `database/sql` DSN format for `database.connection_string`.
Compatibility notes:

- `api.provider` defaults to `openapi_http`
- `database.provider` defaults to `postgres`
- `runtime.sql_output_path` defaults to `res.sql`
- `runtime.run_log_path` defaults to `runlog.log`
- `runtime.event_log_path` defaults to `events.log`
- `runtime.full_reload_interval_seconds` defaults to `0` (disabled)
- `control.listen_addr` defaults to `:8080`
- `control.enabled` defaults to `true`
- `control.history_limit` defaults to `20`
- old config files without `provider` or `runtime` sections still work
- string config values support environment-variable expansion via `${VAR}`

ClickHouse adapter notes:

- tables are created with `MergeTree()`
- primary keys from `x-pk` become the table `ORDER BY` key
- nullable scalar columns become `Nullable(...)`
- `TEXT` and `JSONB` values are stored as `String`

## Run

```bash
go run ./cmd/main.go -config conf.yaml
```

Behavior:

1. Load config and instantiate the selected API and DB providers.
2. Load the OpenAPI spec.
3. Build a structured migration plan from fetched API data.
4. Export SQL through the configured DB adapter.
5. Try applying the migration plan to DB.
6. If DB apply fails, write exported SQL to `runtime.sql_output_path`.

By default the process also starts an internal JSON control API on `control.listen_addr`.

If `runtime.full_reload_interval_seconds > 0`:

1. The parser still performs a full fetch of all fetchable operations.
2. It converts the fetched snapshot into desired table contents keyed by primary key.
3. It keeps running and repeats the full fetch every configured interval.
4. Each cycle reconciles parser-managed tables with the latest API snapshot.
5. If a cycle fails, the cycle SQL snapshot is written to `runtime.sql_output_path` and the process keeps polling.

Periodic full reload notes:

- enabled globally from config, not per operation
- every managed table must have PK metadata (`x-pk` or generated link-table PK), otherwise startup fails
- synchronization scope is limited to parser-managed tables from the loaded spec
- `postgres` uses row-level upsert/delete reconciliation inside one DB transaction
- `clickhouse` uses full-table refresh per managed table (`TRUNCATE` + reinsert), so convergence is eventual but not transactional across the whole cycle

## Architecture Overview

The project is now split into four main modules:

- `internal/config`: typed config loading, defaults, runtime paths, provider selection
- `internal/control`: built-in JSON control API for health, status, recent runs, and log tails
- `internal/api`: API connector registry and networking implementations
- `internal/core`: fetch planning, FK resolution, extraction, and migration-plan building
- `internal/db`: DB target registry and provider-specific execution/SQL export/full-sync reconciliation
- `internal/runner`: reusable runtime loop with one-shot/periodic execution, status tracking, and bounded run history

Connectors are compiled into the binary and selected by config.

## Testing

See [`tests.md`](tests.md) for the package-by-package testing strategy and the canonical test command.

## Extension Reference

### `x-res-type` (operation)

Marks a GET operation as fetchable by parser.

Without `x-res-type`, operation is ignored.

### `x-param-data` (operation parameter)

Controls generated request parameter values.

Supported forms:

```yaml
x-param-data:
  type: operation
  operation-id: publicationId
  filter:
    op: in
    values: [2, 3]
```

```yaml
x-param-data:
  type: values
  values: [1, 2, 3]
```

```yaml
x-param-data:
  type: cursor
  cursor: next.cursor
```

```yaml
x-param-data:
  type: offset
  offset:
    start: 0
    increment: 100
```

Rules:

1. Old `x-fk` parameter hints are rejected.
2. `type=operation` pulls values published by response properties via `x-response-data.id`.
3. `type=values` uses only local `values`.
4. `type=operation` supports optional `filter` with `in | gt | gte | lt | lte`.
5. At most one pagination param (`cursor` or `offset`) is allowed per operation.
6. Cursor pagination stops when the configured dot-path is missing, null, or empty.
7. Offset pagination starts at `offset.start`, increments by `offset.increment`, and stops when a page produces no insertable rows.

### `x-response-data` (response property)

Publishes response values for downstream `x-param-data.type=operation` parameters.

```yaml
x-response-data:
  id: publicationId
```

### `x-auth` (operation parameter)

Maps a parameter value to an environment variable.

Supported form:

```yaml
parameters:
  - name: X-API-Key
    in: header
    required: true
    x-auth: API_TOKEN
    schema:
      type: string
```

Rules:

1. `x-auth` must be a non-empty string.
2. Supported parameter locations are `header` and `query`.
3. The value is read from the named environment variable for every request.
4. If a parameter has both `x-auth` and `x-param-data`, `x-auth` wins.
5. If the env var is missing, that operation is skipped and a warning is logged.
6. Auth-backed parameter values are always redacted in request logs.

### `x-pk` (response property)

Marks a property as primary key.

Used in both:

- classic top-level table mode
- `x-table-name` marked mode

When periodic full reload is enabled, every managed table must have a primary key so rows can be updated and deleted during reconciliation.

### `x-table-name` (response object schema)

Marks nested object/array item object that should be extracted as a separate table.

If at least one `x-table-name` exists in an operation response schema, parser switches that operation to **marked-only mode**:

1. Only marked objects are materialized as tables.
2. Each marked object must have exactly one `x-pk`.
3. Nested relations:
   - object -> object: parent stores child PK
   - object -> array: parent stores typed PK array
   - array -> object: parent row stores child PK
   - array -> array: join table `<parent>_<child>_link`
4. Unmarked containers are ignored for table generation in marked mode.

## Dependency Resolution Model

For fetchable operations (`x-res-type`):

1. Build operation parameter specs from `x-param-data`.
2. Resolve auth-backed parameter specs from `x-auth`.
3. For each op, build candidate lists for `operation` and `values` params.
4. Build cartesian product of candidate lists.
5. Request operation for each combination, injecting auth values from env when configured.
6. If pagination is configured, keep requesting that combination until pagination stops.
7. Extract response-published values from `x-response-data` and feed downstream operations.

Operations without any combination-producing `x-param-data` are called once with empty parameter combination.

## Request Parameter Serialization Notes

For generated parameter values:

- values are converted via `fmt.Sprintf("%v", value)` before request build
- query params are set with `url.Values.Set(...)`

Important for arrays:

- array value like `["BTCUSDT","BNBBTC"]` is currently serialized in Go default style (e.g. `[BTCUSDT BNBBTC]`), not JSON string automatically.
- if API expects JSON array string in query, provide it as a single string seed value, for example:
  - `values: ["[\"BTCUSDT\",\"BNBBTC\"]"]`

## Run Log

Each HTTP request appends one line to `runtime.run_log_path`:

- timestamp (RFC3339)
- method
- URL
- params
- status code
- transport error (if request failed before response)

Parameters resolved through `x-auth` are logged as `***`.

Example line:

```text
2026-03-01T12:34:56Z method=GET url=https://api.binance.com/api/v3/exchangeInfo?symbols=%5BBTCUSDT+BNBBTC%5D params={symbols=[BTCUSDT BNBBTC]} status=200
```

## Event Log

Runtime lifecycle events and non-request failures are written as JSON lines to `runtime.event_log_path`.

Example line:

```json
{"timestamp":"2026-03-24T09:00:00Z","level":"error","kind":"cycle_failed","message":"cycle failed","fields":{"cycle":2,"error":"db unavailable"}}
```

## Control API

The built-in control API is intended for localhost or trusted private networks only.
V1 ships without authentication.

Endpoints:

- `GET /healthz`
- `GET /v1/status`
- `GET /v1/runs`
- `GET /v1/logs?source=requests|events&tail=N`

`/v1/status` returns:

- `job_mode`
- `phase`
- `cycle`
- `started_at`
- `finished_at`
- `next_run_at`
- `request_count`
- `failed_request_count`
- `managed_table_count`
- `planned_row_count`
- `applied_statement_count`
- `last_error`
- `last_success_at`

Phases:

- `starting`
- `running_fetch`
- `running_apply`
- `sleeping`
- `completed`
- `failed`
- `stopping`

Recent run history is kept in memory only and resets on process restart.

## Operator CLI

`apictl` is the operator-facing CLI for Docker packaging and runtime inspection.

Build it:

```bash
go build -o bin/apictl ./cmd/apictl
```

Build a job-specific Docker image with a bundled OpenAPI file and config:

```bash
./bin/apictl image build --tag api-parser:github --openapi example/github.json --config conf.yaml
```

Notes:

- the image bakes the selected OpenAPI spec and a rewritten config into `/app/bundle`
- `openapi_path` is rewritten to the bundled spec path automatically
- keep secrets in environment variables and reference them from config with `${VAR}`

Run the container and expose the control API:

```bash
./bin/apictl run --image api-parser:github --name api-parser-github --port 8080 --env API_TOKEN=secret
```

Inspect the running parser:

```bash
./bin/apictl status --addr http://127.0.0.1:8080
./bin/apictl runs --addr http://127.0.0.1:8080
./bin/apictl logs --addr http://127.0.0.1:8080 --source events --tail 20
```

## Retry Policy

- `5xx` responses are retried up to `api.retries.errors_max_retries` times.
- Delay for `5xx` retries is fixed: `api.retries.basic_retry_timeout`.
- `429 Too Many Requests` retries use exponential backoff with base `api.retries.basic_retry_timeout`.
  - retry delays: `Y`, `2Y`, `4Y`, ...
- Other non-2xx responses are not retried.

## Outputs

- SQL export file at `runtime.sql_output_path` when DB apply fails
- SQL export file at `runtime.sql_output_path` when a periodic full-reload cycle fails
- DB migrations applied directly when DB execution succeeds
- request trace log at `runtime.run_log_path`
- runtime event log at `runtime.event_log_path`
- live status and recent run history through the control API

## Project Examples

- `example/api.json`: minimal dependency chain sample
- `example/binance.json`: real-world endpoint with `x-param-data` seeds/pagination + `x-table-name`
