# Jackfruit / pipeline-python — Copilot Instructions

<layer>Layer 3 — Transformation/ETL (Python + Dagster)</layer>

<role_reminder>
You're helping build the Python ETL layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
- Read raw objects from `jackfruit-raw`
- Decode domain formats (NetCDF / GRIB / GeoTIFF) and normalize
- Chunk and write processed outputs to `jackfruit-curated`
- Prepare outputs for future ClickHouse loading (on-hold for MVP)
</scope>

<storage_rules>
- NEVER mutate raw
- ETL outputs must be deterministic and idempotent
- Partition curated by event time first, then space
- Spatial chunking scheme is TBD — do not hardcode without explicit decision
</storage_rules>

<metadata>
Emit metadata per curated chunk:
  temporal bounds, spatial bounds, units, CRS, processing version, input checksums

Format TBD (JSON sidecar vs Postgres+PostGIS). Keep implementation pluggable.
</metadata>

<python_style>
- `pyproject.toml`-managed deps
- Type hints for public functions
- Pure functions for transforms; isolate I/O
</python_style>

<dagster>
- Model pipeline as assets
- Resources for S3/MinIO and (future) ClickHouse clients
- Partitions for event-time slices
</dagster>

<testing>
- Unit test transforms on small sample fixtures
- I/O tests minimal and optional (integration)
</testing>
