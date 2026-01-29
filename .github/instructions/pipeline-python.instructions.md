# Jackfruit / pipeline-python — Copilot Instructions

<layer>Layers 1 + 3 — Ingestion (future) + Transformation/ETL (Python + Dagster)</layer>

<role_reminder>
You're helping build the Python pipeline layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
**Transformation (active):**
- Read raw objects from `jackfruit-raw`
- Decode GRIB files with `grib2io` (requires PDT 4.40 monkey-patch for CAMS data)
- Extract grid data (lat, lon, value arrays)
- Batch insert into ClickHouse

**Ingestion (planned):**
- Python-native ingestion using `cdsapi` will replace Go ingestion
- Same storage contract: write raw bytes to `jackfruit-raw`
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

<metadata>
Metadata stored in Dagster `MaterializeResult.metadata`:
- run_id, raw_key, variables_processed, rows_inserted, processing_version

Lineage tracked in Postgres `catalog.curated_data` table.
</metadata>

<python_style>
- `pyproject.toml`-managed deps (uv for package management)
- always verify latest dependency versions online before adding anything to pyproject.toml
- Type hints for public functions
- Pure functions for transforms; isolate I/O
</python_style>

<dagster>
- Model pipeline as assets
- Resources for S3/MinIO (`ObjectStorageResource`) and Docker ingestion client
- Lineage via upstream asset metadata (run_id → construct raw key)
- No S3 LIST operations — direct GET by constructed key
</dagster>

<grib2io>
CAMS air quality data uses PDT 4.40 (Atmospheric Chemical Constituents) which grib2io 2.6.0 doesn't support.
A monkey-patch in `scripts/grib_sanity_check.py` adds this support — extract to shared module if needed in ETL.

Note: grib2io is used for READING raw GRIB files only. Output goes to ClickHouse, not GRIB2 files.
</grib2io>

<testing>
- Unit test transforms on small sample fixtures
- I/O tests minimal and optional (integration)
</testing>
