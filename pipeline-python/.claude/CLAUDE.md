# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Dagster-based data pipeline for orchestrating ingestion (Go CLI via Docker) and transformation (Python) of environmental data. Reads raw GRIB files from MinIO, decodes them, and writes curated grid data to ClickHouse.

## Commands

```bash
uv sync                              # Install dependencies
uv run dg dev                        # Start Dagster dev server (localhost:3000)
make test                            # Run all tests
make test-unit                       # Unit tests only
make test-integration                # Integration tests only (requires real infra)
uv run pytest tests/unit/test_resources.py # Run single test file
uv run pytest -k "test_schedule"     # Run tests matching pattern

```

Package manager is **uv** (not pip). Dependencies in `pyproject.toml`, lockfile is `uv.lock`. Always verify latest dependency versions online before adding to pyproject.toml.

## Architecture

### Dagster Structure

Entry point: `src/pipeline_python/definitions.py` uses `load_from_defs_folder` to auto-discover definitions from `defs/`.

```
src/pipeline_python/
├── definitions.py          # Dagster definitions loader
├── defs/
│   ├── assets.py           # ingest_cams_data, transform_cams_data
│   ├── resources.py        # DockerIngestionClient, ObjectStorageResource, PostgresCatalogResource
│   ├── schedules.py        # cams_daily_schedule (08:00 UTC daily)
│   ├── partitions.py       # Daily partitions (start 2026-01-01, UTC, end_offset=1)
│   └── models.py           # RawFileRecord, CuratedDataRecord (frozen dataclasses)
└── grib2/
    ├── reader.py            # GribReader / GribMessage Protocols
    └── adapters/
        └── cams_adapter.py  # CamsReader + CamsMessage (pygrib-backed)
```

### Asset Pipeline

```
ingest_cams_data (Go CLI via Docker)
  → Generates UUIDv7 run_id
  → Spawns ingestion container via PipesDockerClient
  → Records metadata in Postgres catalog
  → Outputs run_id + dataset in MaterializeResult.metadata
        ↓
transform_cams_data (Python)
  → Reads upstream metadata to construct exact raw S3 key (no LIST operations)
  → Downloads raw GRIB from MinIO
  → Opens with pygrib via CamsReader (GribReader Protocol)
  → Extracts grid data per message
  → Writes curated output + records lineage in catalog
```

### Resources

- **DockerIngestionClient** — Spawns Go ingestion container as Docker sibling (not nested). Forwards `ADS_*` and `MINIO_*` env vars. Network: `jackfruit_jackfruit`.
- **ObjectStorageResource** — boto3-based S3/MinIO client. Methods: `download_raw()`. pygrib requires local files, so raw data is downloaded to temp files.
- **PostgresCatalogResource** — psycopg3 client with context manager for connection reuse. `insert_raw_file()` uses `ON CONFLICT DO NOTHING`, `insert_curated_data()` uses `ON CONFLICT DO UPDATE`.

### Grid Storage Abstraction

Grid storage uses the `GridStore` abstract base class (`src/pipeline_python/storage/grid_store.py`):
- `ClickHouseGridStore` (`storage/clickhouse_grid_store.py`) — production Dagster resource, registered as `"grid_store"`

`GridStore` extends `dg.ConfigurableResource` (not a Protocol — Protocols can't carry Dagster config). Transform code depends on the abstract base class, not ClickHouse directly. See [ADR 001](../docs/ADR/001-grid-data-storage.md) for storage decision context.

### Error Handling

- **Fatal:** Missing upstream asset, invalid GRIB file, download failure
- **Non-fatal:** Catalog insert failures (logged as warning, asset continues), individual message write failures
- Assets either succeed completely or fail completely (fail-fast, no partial ClickHouse inserts)
- Catalog is a derived lineage index; ClickHouse is source of truth for grid data

### Database Schema

Postgres schema initialized via `migrations/postgres/init.sql` (mounted into container at startup):
- `catalog.raw_files` — tracks ingested files (id=UUIDv7 run_id, s3_key is UNIQUE)
- `catalog.curated_data` — transformation lineage (id=UUIDv7 catalog_id referenced by CH, raw_file_id FK, variable+timestamp for serving)

## Testing Patterns

Tests use mock Dagster resources and `dg.materialize()` for asset testing:

```python
result = dg.materialize(
    assets=[ingest_cams_data],
    resources={"ingestion_client": mock_client, "catalog": mock_catalog},
    partition_key="2026-01-15",
)
assert result.success
```

Integration tests in `tests/integration/` test the full asset pipeline against real infrastructure.

## Conventions

- Type hints for public functions
- Pure functions for transforms; isolate I/O
- Dagster resources for all external dependencies (S3, Postgres, Docker)
- Frozen dataclasses for domain models
- No S3 LIST operations — construct keys directly from upstream metadata
- Operations must be deterministic and idempotent
- Schedule run_keys include date for idempotency (`cams_daily_{partition_key}`)
