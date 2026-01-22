# Jackfruit / pipeline-python — Copilot Instructions

<layer>Layers 1 + 3 — Ingestion (future) + Transformation/ETL (Python + Dagster)</layer>

<role_reminder>
You're helping build the Python pipeline layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
**Transformation (active):**
- Read raw objects from `jackfruit-raw`
- Decode GRIB files with `grib2io` (requires PDT 4.40 monkey-patch for CAMS data)
- Split multi-variable files into single-variable, single-timestamp curated files
- Write curated GRIB2 to `jackfruit-curated`

**Ingestion (planned):**
- Python-native ingestion using `cdsapi` will replace Go ingestion
- Same storage contract: write raw bytes to `jackfruit-raw`
</scope>

<storage_rules>
- NEVER mutate raw
- ETL outputs must be deterministic and idempotent

Raw key pattern:
`{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

Curated key pattern (single file per variable per timestamp):
`{variable}/{source}/{year}/{month}/{day}/{hour}/data.grib2`

Curated format: GRIB2 (self-describing, enables direct S3 GET in serving layer)
</storage_rules>

<metadata>
Metadata stored in Dagster `MaterializeResult.metadata`:
- run_id, raw_key, curated_keys, variables_processed, processing_version

GRIB2 files are self-describing (coordinates, units, CRS embedded).
</metadata>

<python_style>
- `pyproject.toml`-managed deps (uv for package management)
- always verify exact dependency versions online before adding anything to pyproject.toml
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
</grib2io>

<testing>
- Unit test transforms on small sample fixtures
- I/O tests minimal and optional (integration)
</testing>
