# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Go HTTP service for querying environmental data from the Jackfruit platform. Queries ClickHouse for grid values at coordinates and optionally Postgres for lineage metadata. ClickHouse client and GridStore abstraction are implemented; environmental query endpoint is next.

## Commands

```bash
go run ./cmd/serving              # Start server (default port 8080)
go build -o bin/serving ./cmd/serving  # Build binary
make test                         # All tests (requires ClickHouse for integration)
make test-short                   # Unit tests only (no infra needed)
```

Go 1.26. Key dependencies: `clickhouse-go/v2` (pure Go, no CGO), `google/uuid`.

## Architecture

### Structure

```
serving-go/
├── cmd/serving/main.go
├── internal/
│   ├── api/
│   │   ├── handler.go                         # HTTP handlers (/health, /v1/environmental)
│   │   ├── handler_test.go
│   │   ├── handler_integration_test.go
│   │   ├── request.go                         # Request parsing/validation
│   │   ├── request_test.go
│   │   └── response.go                        # Response types + JSON helpers
│   ├── clickhouse/
│   │   ├── client.go                          # GridStore implementation (nearest-neighbor query)
│   │   └── client_integration_test.go
│   ├── config/
│   │   └── config.go
│   ├── domain/
│   │   ├── environmental.go                   # Service + VariableResult + ErrVariableNotFound
│   │   ├── environmental_test.go
│   │   └── store.go                           # GridStore interface, GridValue
│   └── testutil/
│       └── clickhouse.go                      # Shared test helpers (NewClient, InsertGridRow)
├── Dockerfile
├── Makefile
└── .dockerignore
```

### Planned (not yet implemented)

```
internal/
└── catalog/
    └── repository.go         # Postgres queries for lineage (guide 12)
```

### Grid Storage Abstraction

Grid storage uses the `GridStore` interface (`internal/domain/store.go`):
- Defined in domain package (consumer-side, idiomatic Go)
- `internal/clickhouse/Client` implements `GridStore`
- Mock implementations for unit testing

Domain service depends on `GridStore`, not ClickHouse directly. See [ADR 001](../docs/ADR/001-grid-data-storage.md) for storage decision context.

### API Contract

- `GET /health` → 204 No Content (liveness check)
- `GET /v1/environmental?lat=&lon=&timestamp=&variables=` → JSON with values + per-variable lineage metadata
- Fails entire request if ANY variable not found (no partial responses)
- Errors return `{"error": "..."}` with HTTP status codes (400, 404, 500)

### ClickHouse Query Pattern

Library: `github.com/ClickHouse/clickhouse-go/v2` (pure Go, no CGO)

Nearest-neighbor via:
```sql
SELECT value, unit, lat, lon, catalog_id, timestamp
FROM grid_data FINAL
WHERE variable = @variable
  AND timestamp = (
    SELECT max(timestamp) FROM grid_data FINAL
    WHERE variable = @variable AND timestamp <= @timestamp
  )
ORDER BY (lat - @lat) * (lat - @lat) + (lon - @lon) * (lon - @lon)
LIMIT 1
```

One query per variable, fetched in parallel. **Note:** `grid_data` has no `source` column — source lives in Postgres `catalog.raw_files`, joined via `catalog_id`.

## Server Configuration

Env vars:
- `PORT` — HTTP port (default: 8080)
- `CLICKHOUSE_HOST`, `CLICKHOUSE_NATIVE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE` — grid data
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` — lineage metadata

Server timeouts: read 5s, write 10s, idle 60s. Graceful shutdown on SIGINT/SIGTERM with 5s timeout.

## Conventions

- `internal/` for non-exported packages
- Explicit error handling, no panics in request path
- Context propagation for cancellation
- `slog` with JSON handler for structured logging
- Environment variables for all configuration
- Standard library HTTP server (no frameworks) with Go 1.22+ routing (`mux.HandleFunc("GET /path", handler)`)
- Clients never know about S3 paths, bucket structure, or source selection logic
