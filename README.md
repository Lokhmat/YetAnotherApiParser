# API Parser

`api-parser` reads an OpenAPI spec, fetches selected API endpoints, builds a database-agnostic migration plan, and executes it through a configured database adapter.

Current built-in providers:

- API provider: `openapi_http`
- DB provider: `postgres`

With the default Postgres adapter it still generates PostgreSQL SQL such as:

- `CREATE TABLE ...`
- `INSERT INTO ...`

It can:

- resolve endpoint dependencies with `x-fk`
- fetch in dependency order
- flatten marked nested response objects into separate tables (`x-table-name`)
- export SQL through the DB adapter
- apply migrations directly to DB
- write request run logs to a configurable path

## Requirements

- Go 1.22+
- PostgreSQL (optional; if DB is unavailable, SQL is still written to file)

## Configuration

Create `conf.yaml`:

```yaml
openapi_path: "example/api.json"

runtime:
  sql_output_path: "res.sql"
  run_log_path: "runlog.log"

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

Compatibility notes:

- `api.provider` defaults to `openapi_http`
- `database.provider` defaults to `postgres`
- `runtime.sql_output_path` defaults to `res.sql`
- `runtime.run_log_path` defaults to `runlog.log`
- old config files without `provider` or `runtime` sections still work

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

## Architecture Overview

The project is now split into four main modules:

- `internal/config`: typed config loading, defaults, runtime paths, provider selection
- `internal/api`: API connector registry and networking implementations
- `internal/core`: fetch planning, FK resolution, extraction, and migration-plan building
- `internal/db`: DB target registry and provider-specific execution/SQL export

Connectors are compiled into the binary and selected by config.

## Extension Reference

### `x-res-type` (operation)

Marks a GET operation as fetchable by parser.

Without `x-res-type`, operation is ignored.

### `x-fk` (operation parameter)

Controls dependency-driven parameter value generation.

Supported forms:

```yaml
# Simple mode
x-fk: true
```

```yaml
# Extended mode
x-fk:
  id: publicationId     # optional, defaults to parameter name
  values: [1, 2, 3]     # optional seed values (scalars or arrays)
  filter:               # optional
    op: in              # in | gt | gte | lt | lte
    values: [2, 3]      # required for op=in
```

Rules:

1. `x-fk: "..."` string format is rejected for parameters.
2. Candidate values per FK param are:
   - fetched values by `id`
   - union with optional `values`
3. Filter is applied before cartesian product.
4. `gt/gte/lt/lte` support:
   - numeric values
   - strict RFC3339 datetime strings
5. If any FK param has an empty candidate set after filtering, operation is skipped (no requests).

### `x-pk` (response property)

Marks a property as primary key.

Used in both:

- classic top-level table mode
- `x-table-name` marked mode

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

1. Build operation FK specs from parameter `x-fk`.
2. For each op, build FK candidate lists.
3. Build cartesian product of FK lists.
4. Request operation for each combination.
5. Extract FK values from responses and feed downstream operations.

Operations without any `x-fk` are called once with empty parameter combination.

## Request Parameter Serialization Notes

For generated FK parameter values:

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

Example line:

```text
2026-03-01T12:34:56Z method=GET url=https://api.binance.com/api/v3/exchangeInfo?symbols=%5BBTCUSDT+BNBBTC%5D params={symbols=[BTCUSDT BNBBTC]} status=200
```

## Retry Policy

- `5xx` responses are retried up to `api.retries.errors_max_retries` times.
- Delay for `5xx` retries is fixed: `api.retries.basic_retry_timeout`.
- `429 Too Many Requests` retries use exponential backoff with base `api.retries.basic_retry_timeout`.
  - retry delays: `Y`, `2Y`, `4Y`, ...
- Other non-2xx responses are not retried.

## Outputs

- SQL export file at `runtime.sql_output_path` when DB apply fails
- DB migrations applied directly when DB execution succeeds
- request trace log at `runtime.run_log_path`

## Project Examples

- `example/api.json`: minimal dependency chain sample
- `example/binance.json`: real-world endpoint with `x-fk` seeds + `x-table-name`
