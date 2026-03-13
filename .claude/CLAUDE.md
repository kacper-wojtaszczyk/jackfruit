# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Jackfruit is an environmental data platform that ingests, transforms, and serves gridded environmental data (air quality, weather, hydrology, vegetation) from sources like Copernicus CAMS, GloFAS, and ERA5.

## Infrastructure Commands

```bash
cp .env.example .env              # First time: configure secrets
docker-compose up -d              # Start MinIO, Postgres, ClickHouse, Dagster
# Dagster UI: http://localhost:3099
# MinIO console: http://localhost:9098 (minioadmin/minioadmin)
```

## Architecture

Three processing layers with strict boundaries — do not blur them:

```
External APIs (Copernicus ADS, etc.)
        ↓
  L1: Ingestion (pipeline-python/)
  Python + cdsapi fetches raw data → MinIO jackfruit-raw bucket
        ↓
  L2: Transformation (pipeline-python/)
  Dagster assets: read raw GRIB → decode → extract grids → ClickHouse
        ↓
  L3: Serving (serving-go/)
  Go HTTP API → query ClickHouse → JSON responses
```

**Infrastructure services** (docker-compose.yml):
- **MinIO** — S3-compatible object storage for raw data (ports 9099/9098)
- **Postgres** — Metadata catalog with `catalog` schema for lineage (port 5432)
- **ClickHouse** — Columnar store for curated grid data (ports 8123/9097)
- **Dagster** — Orchestration (port 3099)

### Key architectural decisions

- Raw bucket (`jackfruit-raw`) is **immutable, append-only** — ETL reads raw, writes to ClickHouse
- Raw key pattern: `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`
- ClickHouse stores curated grid data as rows: (variable, timestamp, lat, lon, value, unit, catalog_id)
- **Grid storage is abstracted** — both Python (`GridStore` abstract base class in `storage/grid_store.py`) and Go (`GridStore` interface) depend on abstractions, not ClickHouse directly. The Python `ClickHouseGridStore` is registered as a Dagster resource (`"grid_store"`). See [ADR 001](docs/ADR/001-grid-data-storage.md).
- **No `source` column in `grid_data`** — source lives in Postgres `catalog.raw_files` only (joined via `catalog_id`)

## Conventions

- Operations should be **deterministic and idempotent**
- Small, composable modules
- Always verify latest dependency versions online before adding to pyproject.toml
- No S3 LIST operations in pipeline — direct GET by constructed key

---

## Sub-project: serving-go

Go HTTP service that queries ClickHouse for grid values at coordinates and optionally Postgres for lineage metadata.

### Commands

```bash
go run ./cmd/serving              # Start server (default port 8080)
go build -o bin/serving ./cmd/serving  # Build binary
make test                         # All tests (requires ClickHouse for integration)
make test-short                   # Unit tests only (no infra needed)
```

Go 1.26. Key dependencies: `clickhouse-go/v2` (pure Go, no CGO), `google/uuid`.

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

Planned (not yet implemented):
```
internal/catalog/repository.go    # Postgres lineage queries
```

### API Contract

- `GET /health` → 204 No Content
- `GET /v1/environmental?lat=&lon=&timestamp=&variables=` → JSON with values + per-variable lineage metadata
- Fails entire request if ANY variable not found (no partial responses)
- Errors: `{"error": "..."}` with HTTP status codes (400, 404, 500)

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

One query per variable, fetched in parallel. `grid_data` has no `source` column — source lives in Postgres `catalog.raw_files`, joined via `catalog_id`.

### Server Configuration

Env vars: `PORT`, `CLICKHOUSE_HOST`, `CLICKHOUSE_NATIVE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`.

Server timeouts: read 5s, write 10s, idle 60s. Graceful shutdown on SIGINT/SIGTERM (5s timeout).

### Conventions

- `internal/` for non-exported packages
- Explicit error handling, no panics in request path
- Context propagation for cancellation
- `slog` JSON handler for structured logging
- Standard library HTTP server (no frameworks), Go 1.22+ routing: `mux.HandleFunc("GET /path", handler)`
- GridStore abstraction — consumers never depend on ClickHouse directly (see root CLAUDE.md above)

---

## Sub-project: pipeline-python

Dagster-based data pipeline: ingestion + transformation (Python) of GRIB environmental data. Reads raw files from MinIO/S3, decodes, writes curated grid data to ClickHouse.

### Commands

```bash
uv sync                                          # Install dependencies
uv run dg dev                                    # Start Dagster dev server (localhost:3000)
make test                                        # Run all tests
make test-unit                                   # Unit tests only
make test-integration                            # Integration tests (requires real infra)
uv run pytest tests/unit/test_resources.py       # Run single test file
uv run pytest -k "test_schedule"                 # Run tests matching pattern
```

Package manager: **uv** (not pip). Dependencies in `pyproject.toml`, lockfile `uv.lock`. Always verify latest dependency versions online before adding.

### Dagster Structure

Entry point: `src/pipeline_python/definitions.py` — uses `load_from_defs_folder` to auto-discover definitions from `defs/`.

```
src/pipeline_python/
├── definitions.py
├── defs/
│   ├── assets.py           # ingest_cams_data, transform_cams_data
│   ├── resources.py        # Wires CdsClient, ObjectStore, PostgresCatalogResource, ClickHouseGridStore
│   ├── schedules.py        # cams_daily_schedule (08:00 UTC daily)
│   ├── partitions.py       # Daily partitions (start 2026-01-01, UTC, end_offset=1)
│   └── models.py           # RawFileRecord, CuratedDataRecord (frozen dataclasses)
├── ingestion/
│   └── cds_client.py       # CdsClient (ConfigurableResource wrapping cdsapi)
├── storage/
│   ├── grid_store.py       # GridStore ABC + GridData dataclass
│   ├── clickhouse_grid_store.py  # ClickHouseGridStore implementation
│   └── object_store.py     # ObjectStore (ConfigurableResource, boto3-based S3/MinIO)
└── grib2/
    ├── reader.py            # GribReader / GribMessage Protocols
    └── adapters/
        └── cams_adapter.py  # CamsReader + CamsMessage (pygrib-backed)
```

### Asset Pipeline

```
ingest_cams_data (Python)
  → Generates UUIDv7 run_id
  → Calls CDS API via CdsClient, uploads to MinIO via ObjectStore
  → Records metadata in Postgres catalog
        ↓
transform_cams_data (Python)
  → Constructs exact S3 key from upstream metadata (no LIST operations — see root Conventions)
  → Downloads raw GRIB from MinIO to temp file (pygrib requires local files)
  → Decodes via CamsReader / pygrib
  → Writes curated output + lineage to ClickHouse + catalog
```

### Resources

- **CdsClient** — `ConfigurableResource` wrapping `cdsapi`. `retrieve_forecast()` downloads GRIB from Copernicus ADS to a local temp path.
- **ObjectStore** — boto3-based S3/MinIO client. `download_raw()` downloads to temp files; `upload_raw()` uploads to the raw bucket.
- **PostgresCatalogResource** — psycopg3. `insert_raw_file()` uses `ON CONFLICT DO NOTHING`; `insert_curated_data()` uses `ON CONFLICT DO UPDATE`.

Grid storage: `GridStore` abstract base class (`storage/grid_store.py`). `ClickHouseGridStore` is the production implementation, registered as Dagster resource `"grid_store"`. Transform code depends on the ABC, not ClickHouse directly.

### Error Handling

- **Fatal:** missing upstream asset, invalid GRIB file, download failure, catalog insert failure
- Assets either succeed completely or fail completely — no partial ClickHouse inserts
- Catalog writes are fail-fast in both ingestion and transformation (licensing compliance requires lineage)

### Database Schema

Postgres schema in `migrations/postgres/init.sql`:
- `catalog.raw_files` — ingested files (id = UUIDv7 run_id, s3_key UNIQUE)
- `catalog.curated_data` — transformation lineage (id = UUIDv7 catalog_id referenced by ClickHouse, raw_file_id FK)

### Testing Patterns

```python
result = dg.materialize(
    assets=[ingest_cams_data],
    resources={"cds_client": mock_client, "object_store": mock_store, "catalog": mock_catalog},
    partition_key="2026-01-15",
)
assert result.success
```

Integration tests in `tests/integration/` test against real infrastructure.

### Conventions

- Type hints for public functions
- Pure functions for transforms; isolate I/O
- Dagster resources for all external dependencies (S3, Postgres, ClickHouse, CDS API)
- Frozen dataclasses for domain models
- Idempotent ops and no S3 LIST — see root Conventions above
- Schedule run_keys include date for idempotency (`cams_daily_{partition_key}`)
