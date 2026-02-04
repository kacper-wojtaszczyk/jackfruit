# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Jackfruit is an environmental data platform that ingests, transforms, and serves gridded environmental data (air quality, weather, hydrology, vegetation) from sources like Copernicus CAMS, GloFAS, and ERA5.

## Build & Development Commands

### Infrastructure

```bash
cp .env.example .env              # First time: configure secrets
docker-compose up -d              # Start MinIO, Postgres, ClickHouse, Dagster
# Dagster UI: http://localhost:3099
# MinIO console: http://localhost:9098 (minioadmin/minioadmin)
```

### ingestion-go (Go CLI)

```bash
cd ingestion-go
make build                        # Build to bin/ingestion
make test                         # Unit tests (go test -short -cover ./...)
make test-integration             # All tests including integration (requires MinIO)
make run ARGS="--dataset=... --date=... --run-id=..."
```

### pipeline-python (Dagster + Python ETL)

```bash
cd pipeline-python
uv sync                           # Install dependencies
uv run dagster dev                # Start Dagster dev server
uv run pytest                     # Run tests
```

Package manager is **uv** (not pip). Dependencies in `pyproject.toml`, lockfile is `uv.lock`.

### serving-go (HTTP API)

```bash
cd serving-go
go run ./cmd/serving              # Start server
go test ./...                     # Run tests
```

## Architecture

Three processing layers with strict boundaries — do not blur them:

```
External APIs (Copernicus ADS, etc.)
        ↓
  L1: Ingestion (ingestion-go/)
  Go CLI fetches raw data → MinIO jackfruit-raw bucket
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
- **Dagster** — Orchestration (port 3099, mounts host Docker socket)

### Key architectural decisions

- Raw bucket (`jackfruit-raw`) is **immutable, append-only** — ETL reads raw, writes to ClickHouse
- Raw key pattern: `{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`
- ClickHouse stores curated grid data as rows: (variable, source, timestamp, lat, lon, value)
- Serving API queries ClickHouse with nearest-neighbor: `ORDER BY greatCircleDistance(...) LIMIT 1`
- One query per variable, fetched in parallel via goroutines + errgroup
- CAMS GRIB data uses PDT 4.40 which requires a monkey-patch in `pipeline-python/src/pipeline_python/grib2/pdt40.py`
- `grib2io` pinned to 2.6.0 (2.7.0 has a circular import bug)

### Serving API contract

- `GET /health` → 204 No Content
- `GET /v1/environmental?lat=&lon=&time=&vars=` → JSON with values + per-variable lineage metadata
- Fails entire request if ANY variable not found (no partial responses)

## Conventions

- **Go:** `internal/` packages, explicit error handling (no panics in request path), context propagation, `slog` for structured logging (JSON), env vars for config
- **Python:** Type hints for public functions, pure functions for transforms, isolated I/O, Dagster resources for external dependencies
- Operations should be **deterministic and idempotent**
- Small, composable modules
- Always verify latest dependency versions online before adding to pyproject.toml
- No S3 LIST operations in pipeline — direct GET by constructed key