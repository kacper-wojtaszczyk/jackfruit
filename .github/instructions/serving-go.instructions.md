# Jackfruit / serving-go — Copilot Instructions

<layer>Layer 3 — Serving API (Go)</layer>

<role_reminder>
You're helping build the Go serving layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
**Serving (active):**
- HTTP API for querying environmental data (any gridded variable: air quality, temperature, vegetation, etc.)
- Query ClickHouse for grid values at coordinates
- Optionally query Postgres `catalog.curated_data` + `catalog.raw_files` for lineage
- Return JSON responses with values + per-variable lineage metadata

> **MVP Scope:** Initial implementation uses air quality variables (`pm2p5`, `pm10`) from CAMS. The architecture is variable-agnostic.

**Does NOT do:**
- Fetch from external APIs (that's ingestion)
- Transform data (that's ETL)
- Read from S3 curated bucket (data is in ClickHouse)
</scope>

<api_contract>
**Endpoints:**
- `GET /health` → 204 No Content (liveness)
- `GET /v1/environmental?lat=&lon=&timestamp=&variables=` → JSON with values + metadata

**Request parameters:**
- `lat`, `lon`: coordinates (float)
- `timestamp`: ISO8601 timestamp (UTC)
- `variables`: comma-separated variable names (e.g., `pm2p5,pm10,temperature`)

**Response shape:**
```json
{
  "lat": 52.52,
  "lon": 13.40,
  "requested_timestamp": "2025-03-12T14:55:00Z",
  "variables": [
    {
      "name": "pm2p5",
      "value": 12.34,
      "unit": "µg/m³",
      "ref_timestamp": "2025-03-12T14:00:00Z",
      "actual_lat": 52.50,
      "actual_lon": 13.50,
      "lineage": {
        "source": "ads",
        "dataset": "cams-europe-air-quality-forecast",
        "raw_file_id": "..."
      }
    }
  ]
}
```

> `lineage` object is planned (guide 12) — not yet implemented. `catalog_id` is internal only and never exposed.

**Error behavior:**
- Fail entire request if ANY variable not found (no partial responses)
- Return JSON error with `error` message
</api_contract>

<boundaries>
- Clients know ONLY: coordinates, timestamp, variable names
- Clients do NOT know: S3 paths, source selection logic, bucket structure
- Source selection is hardcoded in Go (CAMS for MVP)
- Lineage metadata is ALWAYS included in responses (per-variable)
</boundaries>

<clickhouse>
**Library:** `github.com/ClickHouse/clickhouse-go/v2`

**Why ClickHouse:**
- SQL queries replace complex GRIB parsing
- Built-in nearest-neighbor via `ORDER BY distance LIMIT 1`
- No CGO dependencies (pure Go driver)
- Excellent query performance on grid data

**Query pattern (nearest-neighbor with timestamp snapping):**
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

No `source` column in `grid_data` — source lives in Postgres `catalog.raw_files`, joined via `catalog_id`.
</clickhouse>

<grid_storage_abstraction>
Grid storage is accessed via `GridStore` interface (`internal/domain/store.go`):
- Interface defined in domain package (consumer-side, idiomatic Go)
- `internal/clickhouse/Client` implements `GridStore`
- Mock implementations for unit testing

Domain service depends on `GridStore`, not ClickHouse directly. This enables testing without external dependencies and future storage swaps.

See [ADR 001](docs/ADR/001-grid-data-storage.md) for storage decision context.
</grid_storage_abstraction>

<database>
**ClickHouse (grid data):**
- Primary data source for environmental values
- One query per variable (parallel goroutines)
- Nearest-neighbor via `ORDER BY distance LIMIT 1`

**Postgres (lineage — optional):**
- Query `catalog.curated_data` joined with `catalog.raw_files` for lineage
- May be skipped for performance if lineage not needed in response

**Timestamp snapping:**
- Snap to last datapoint BEFORE requested timestamp
- Tolerance window: TBD (experiment during implementation)
</database>

<go_style>
- `internal/` for non-exported packages
- Explicit error handling, no panics in request path
- Context propagation for cancellation
- Parallel variable fetching via goroutines + errgroup
- slog for structured logging (JSON format)
- Environment variables for configuration
</go_style>

<project_structure>
```
serving-go/
├── cmd/serving/main.go      # Entry point
├── internal/
│   ├── api/                 # HTTP handlers, request/response
│   ├── clickhouse/          # ClickHouse client (implements GridStore)
│   ├── catalog/             # Postgres repository for lineage
│   ├── domain/
│   │   ├── store.go         # GridStore interface
│   │   └── environmental.go # Business logic
│   └── config/              # Environment config
```
</project_structure>

<testing>
- Unit test handlers with mock ClickHouse/catalog
- Integration tests optional (real Postgres + ClickHouse)
- Test error paths: missing variable, invalid coords, ClickHouse failure
</testing>
