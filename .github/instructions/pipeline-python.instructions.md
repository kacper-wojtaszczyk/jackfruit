# Jackfruit / pipeline-python — Copilot Instructions

<layer>Layers 1 + 2 — Ingestion + Transformation/ETL (Python + Dagster)</layer>

<role_reminder>
You're helping build the Python pipeline layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
**Ingestion (active):**
- Python-native ingestion using `cdsapi` via `CdsClient` ConfigurableResource
- Downloads GRIB from Copernicus ADS, uploads to MinIO via `ObjectStore`
- Records metadata in Postgres catalog

**Transformation (active):**
- Read raw objects from `jackfruit-raw`
- Decode GRIB files with `pygrib` via `CamsReader` adapter (Protocol-based, see ADR 002)
- Extract grid data (lat, lon, value arrays)
- Unit conversion (kg m-3 → µg/m³)
- Batch insert into ClickHouse via `GridStore` abstraction
</scope>

<storage_rules>
- NEVER mutate raw
- ETL outputs must be deterministic and idempotent

Raw key pattern:
`{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

Curated output: ClickHouse `grid_data` table
- One row per grid point per timestamp per variable
- Batch inserts for efficiency
</storage_rules>

<grid_storage_abstraction>
Grid storage is accessed via the `GridStore` ABC (`src/pipeline_python/storage/grid_store.py`):
- `ClickHouseGridStore` (`storage/clickhouse_grid_store.py`) — production implementation

Transform code should depend on `GridStore`, not ClickHouse directly. This enables future storage swaps.

See [ADR 001](docs/ADR/001-grid-data-storage.md) for storage decision context.
</grid_storage_abstraction>

<metadata>
Metadata stored in Dagster `MaterializeResult.metadata`:
- Ingestion: run_id, source, dataset, date
- Transform: run_id, date, curated_keys, variables_processed, inserted_rows

Lineage tracked in Postgres `catalog.curated_data` table (catalog_id links CH ↔ Postgres).
</metadata>

<python_style>
- `pyproject.toml`-managed deps (uv for package management)
- always verify latest dependency versions online before adding anything to pyproject.toml
- Type hints for public functions
- Pure functions for transforms; isolate I/O
</python_style>

<dagster>
- Model pipeline as assets (partitioned daily)
- Resources: `CdsClient`, `ObjectStore`, `PostgresCatalogResource`, `ClickHouseGridStore` (as `GridStore`)
- Lineage via upstream asset metadata (run_id → construct raw key)
- No S3 LIST operations — direct GET by constructed key
- Schedule: `cams_daily_schedule` at 08:00 UTC
</dagster>

<grib_reading>
GRIB reading uses `pygrib` (ecCodes backend) via Protocol-based abstraction:
- `GribReader` / `GribMessage` protocols in `grib2/reader.py`
- `CamsReader` adapter in `grib2/adapters/cams_adapter.py`
- Asset code imports protocols, never pygrib directly

See [ADR 002](docs/ADR/002-grib-library.md) for the migration from grib2io.
</grib_reading>

<testing>
- Unit test transforms on small sample fixtures
- I/O tests minimal and optional (integration)
</testing>
