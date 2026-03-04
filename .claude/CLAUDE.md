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
- ClickHouse stores curated grid data as rows: (variable, timestamp, lat, lon, value, unit, catalog_id)
- **Grid storage is abstracted** — both Python (`GridStore` abstract base class in `storage/grid_store.py`) and Go (`GridStore` interface) depend on abstractions, not ClickHouse directly. The Python `ClickHouseGridStore` is registered as a Dagster resource (`"grid_store"`). See [ADR 001](docs/ADR/001-grid-data-storage.md).
- **No `source` column in `grid_data`** — source lives in Postgres `catalog.raw_files` only (joined via `catalog_id`)

## Conventions

- Operations should be **deterministic and idempotent**
- Small, composable modules
- Always verify latest dependency versions online before adding to pyproject.toml
- No S3 LIST operations in pipeline — direct GET by constructed key
