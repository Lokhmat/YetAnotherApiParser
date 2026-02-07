# API Parser

API Parser is a Go project that automatically fetches API data from OpenAPI specifications and generates PostgreSQL migrations (CREATE TABLE and INSERT statements).

## Features

- Reads OpenAPI (Swagger) specifications from YAML/JSON files
- Automatically generates database schema (CREATE TABLE statements) from API response schemas
- Fetches API data and generates INSERT statements for PostgreSQL
- Supports dependent API calls through `x-fk` and `x-res-type` extensions
- Handles foreign key dependencies between API endpoints

## Configuration

Create a `conf.yaml` file with the following structure:

```yaml
database:
  connection_string: "host=localhost port=5440 user=postgres password=postgres dbname=api_parser"

api:
  base_url: "http://example.com/api"

openapi_path: "path/to/spec.json"
```

## Usage

```bash
go run ./cmd/main.go -config conf.yaml
```

Or build and run:

```bash
go build -o api-parser ./cmd/main.go
./api-parser -config conf.yaml
```

## OpenAPI Extensions

The project uses the following OpenAPI extensions:

- `x-res-type`: Mark handlers that should be fetched (applied to operations)
- `x-fk`: Mark parameters that reference foreign keys from other handlers (applied to parameters)

### How it works

1. Operations without `x-fk` parameters are fetched first, and their response data is used to populate the database
2. When an operation has `x-fk` parameters, the parser looks for matching values in previously fetched data
3. The parser generates all combinations of FK values and fetches data for each combination
4. Only fields defined in the OpenAPI schema are included in INSERT statements (unexpected API fields are ignored)

## Example

See `example/api.json` for an example OpenAPI specification that uses the `x-res-type` and `x-fk` extensions.

## Output

The project outputs:
- `res.sql`: SQL file containing CREATE TABLE and INSERT statements
- Database migrations (if database connection is successful)

## Dependencies

- Go 1.21+
- PostgreSQL (for database migrations)
