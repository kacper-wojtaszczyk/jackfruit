# Jackfruit / ingestion-go — Copilot Instructions

<layer>Layer 1 — Ingestion (Go) — DEPRECATED</layer>

<deprecation>
This Go ingestion layer is functional but being replaced with native Python ingestion using `cdsapi`.
Reasons: container orchestration complexity, parallelism benefits didn't materialize (CDS returns single files), unified Python stack simpler.
Continue maintenance but no major new features. See docs/layer-1-ingestion.md for details.
</deprecation>

<role_reminder>
You're helping maintain the Go ingestion layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
- Fetch data from external sources (CDS API async job polling)
- Write raw bytes to `jackfruit-raw` bucket
- Do NOT parse/transform/normalize beyond what's needed for storage
</scope>

<storage_contract>
Raw key pattern:
`{source}/{dataset}/{YYYY-MM-DD}/{run_id}.{ext}`

Example: `ads/cams-europe-air-quality-forecasts-analysis/2025-03-12/01890c24-905b-7122-b170-b60814e6ee06.grib`

Rules:
- Raw is append-only; NEVER overwrite existing objects
- Run ID (UUIDv7) ensures uniqueness per ingest
- Date in path is ingest date (when fetched), not event date
</storage_contract>

<go_style>
- Small interfaces (e.g., `ObjectStorage`), implicit implementation
- `context.Context` for all I/O
- Streaming I/O (`io.Reader`) over buffering in memory
- Wrap errors with context: `fmt.Errorf("…: %w", err)`
</go_style>

<testing>
- Unit test object key generation and parsing
- In-memory mock storage for most tests
- MinIO integration tests behind build tag if slow
</testing>
