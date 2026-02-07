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
  max_rpm: 60  # Maximum requests per minute (optional, default: 60)

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
- `x-pk`: Mark fields that should be treated as PRIMARY KEY (applied to response properties)

### How it works

1. Operations without `x-fk` parameters are fetched first, and their response data is used to populate the database
2. When an operation has `x-fk` parameters, the parser looks for matching values in previously fetched data
3. The parser generates all combinations of FK values and fetches data for each combination
4. Only fields defined in the OpenAPI schema are included in INSERT statements (unexpected API fields are ignored)

### PRIMARY KEY Support

Use the `x-pk` extension on response properties to mark them as PRIMARY KEY in CREATE TABLE statements:

```yaml
paths:
  /users:
    get:
      x-res-type: user
      responses:
        '200':
          content:
            application/json:
              schema:
                type: object
                properties:
                  id:
                    type: integer
                    x-pk: true
                  name:
                    type: string
```

This generates:

```sql
CREATE TABLE response (
  id INTEGER PRIMARY KEY,
  name TEXT
);
```

### Rate Limiting

The project implements token bucket rate limiting to control the number of requests to external APIs. Configure the maximum requests per minute using `max_rpm` in your configuration:

```yaml
api:
  base_url: "http://example.com/api"
  max_rpm: 30  # Maximum 30 requests per minute (default: 60)
```

## Example

See `example/api.json` for an example OpenAPI specification that uses the `x-res-type` and `x-fk` extensions.

## Output

The project outputs:
- `res.sql`: SQL file containing CREATE TABLE and INSERT statements
- Database migrations (if database connection is successful)

## Dependencies

- Go 1.21+
- PostgreSQL (for database migrations)
