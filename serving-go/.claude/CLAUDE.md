# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Go HTTP service for querying environmental data from the Jackfruit platform. Queries ClickHouse for grid values at coordinates and optionally Postgres for lineage metadata. Currently in early development (health endpoint only).

## Commands

```bash
go run ./cmd/serving              # Start server (default port 8080)
go build -o bin/serving ./cmd/serving  # Build binary
go test ./...                     # Run tests
```

No external dependencies yet — uses only Go standard library. Go 1.25.

## Architecture

### Current State

Minimal skeleton with health endpoint. Entry point: `cmd/serving/main.go`.

```
serving-go/
├── cmd/serving/main.go       # Server bootstrap, graceful shutdown, /health handler
└── internal/config/config.go # PORT env var (default "8080")
```

### Planned Structure

```
internal/
├── api/
│   ├── handler.go            # HTTP handlers
│   ├── request.go            # Request parsing/validation
│   └── response.go           # Response formatting
├── clickhouse/
│   └── client.go             # ClickHouse queries for grid data
├── catalog/
│   └── repository.go         # Postgres queries for lineage
├── domain/
│   └── environmental.go      # Business logic orchestration
└── config/
    └── config.go             # Environment config (exists)
```

### Request Flow (Planned)

1. Parse request — extract coordinates, timestamp, variable list
2. For each variable (in parallel via goroutines + errgroup):
   - Query ClickHouse for value at nearest grid point
   - Optionally query Postgres for lineage metadata
3. Aggregate results
4. Return JSON response

### API Contract

- `GET /health` → 204 No Content (liveness check)
- `GET /v1/environmental?lat=&lon=&time=&vars=` → JSON with values + per-variable lineage metadata
- Fails entire request if ANY variable not found (no partial responses)

Error codes: `INVALID_REQUEST` (400), `VARIABLE_NOT_FOUND` (404), `INTERNAL_ERROR` (500).

### ClickHouse Query Pattern

Library: `github.com/ClickHouse/clickhouse-go/v2` (pure Go, no CGO)

Nearest-neighbor via:
```sql
SELECT value, lat, lon
FROM grid_data
WHERE variable = ? AND source = ? AND timestamp = ?
ORDER BY greatCircleDistance(lat, lon, ?, ?)
LIMIT 1
```

One query per variable, fetched in parallel. Source is hardcoded to CAMS for MVP.

### Postgres Lineage (Optional)

Query `catalog.curated_data` joined with `catalog.raw_files` for per-variable lineage metadata (raw_file_id, source, dataset).

### Timestamp Handling

Snap to last available datapoint before requested timestamp. Tolerance window TBD. Response always includes `ref_timestamp` showing actual data time.

## Server Configuration

Env vars:
- `PORT` — HTTP port (default: 8080)
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE` — grid data
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
