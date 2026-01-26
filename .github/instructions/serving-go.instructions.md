# Jackfruit / serving-go — Copilot Instructions

<layer>Layer 3 — Serving API (Go)</layer>

<role_reminder>
You're helping build the Go serving layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
**Serving (active):**
- HTTP API for querying environmental data (any gridded variable: air quality, temperature, vegetation, etc.)
- Query `catalog.curated_files` + `catalog.raw_files` for file locations and lineage
- Fetch GRIB2 files from `jackfruit-curated` bucket
- Extract values at coordinates using eccodes
- Return JSON responses with values + per-variable lineage metadata

> **MVP Scope:** Initial implementation uses air quality variables (`pm2p5`, `pm10`) from CAMS. The architecture is variable-agnostic.

**Does NOT do:**
- Fetch from external APIs (that's ingestion)
- Transform data (that's ETL)
- Mutate any bucket data
</scope>

<api_contract>
**Endpoints:**
- `GET /health` → 204 No Content (liveness)
- `GET /v1/environmental?lat=&lon=&time=&vars=` → JSON with values + metadata

**Request parameters:**
- `lat`, `lon`: coordinates (float)
- `time`: ISO8601 timestamp (UTC)
- `vars`: comma-separated variable names (e.g., `pm2p5,pm10,temperature`)

**Response shape:**
```json
{
  "location": { "lat": 52.52, "lon": 13.40 },
  "requested_timestamp": "2025-03-12T14:55:00Z",
  "variables": [
    {
      "name": "pm2p5",
      "value": 12.34,
      "unit": "µg/m³",
      "metadata": {
        "ref_timestamp": "2025-03-12T14:00:00Z",
        "raw_file_id": "...",
        "source": "ads",
        "dataset": "cams-europe-air-quality-forecasts-analysis"
      }
    },
    {
      "name": "temperature",
      "value": 285.5,
      "unit": "K",
      "metadata": {
        "ref_timestamp": "2025-03-12T14:00:00Z",
        "raw_file_id": "...",
        "source": "ads",
        "dataset": "reanalysis-era5-single-levels"
      }
    }
  ]
}
```

**Error behavior:**
- Fail entire request if ANY variable not found (no partial responses)
- Return JSON error with `error` message and `code` field
</api_contract>

<boundaries>
- Clients know ONLY: coordinates, timestamp, variable names
- Clients do NOT know: S3 paths, source selection logic, bucket structure
- Source selection is hardcoded in Go (CAMS for MVP)
- Lineage metadata is ALWAYS included in responses (per-variable)
</boundaries>

<grib2>
**Library:** eccodes via CGO

**Why eccodes:**
- ECMWF's official library — handles PDT 4.40 (CAMS atmospheric chemical constituents)
- Built-in `GetDataAtPoint(lat, lon)` — no manual grid math
- Pure Go alternatives don't support PDT 4.40

**Build requirements:**
- macOS: `brew install eccodes`
- Docker: `libeccodes-dev` (build), `libeccodes0` (runtime)
</grib2>

<database>
Query `catalog.curated_files` joined with `catalog.raw_files` for lineage.

**Access pattern:**
- One query per variable (parallel goroutines)
- Find latest curated file ≤ requested timestamp within tolerance
- Join to get raw_file_id, source, dataset for lineage

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
│   ├── catalog/             # Postgres repository
│   ├── domain/              # Business logic (environmental.go)
│   ├── grib/                # eccodes wrapper
│   ├── storage/             # S3/MinIO client
│   └── config/              # Environment config
```
</project_structure>

<testing>
- Unit test handlers with mock catalog/storage
- Integration tests optional (real Postgres + MinIO)
- Test error paths: missing variable, invalid coords, S3 failure
</testing>
