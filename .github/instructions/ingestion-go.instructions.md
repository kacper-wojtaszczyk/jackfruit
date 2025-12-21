# Jackfruit / ingestion-go — Copilot Instructions

<layer>Layer 1 — Ingestion (Go)</layer>

<role_reminder>
You're helping build the Go ingestion layer. Inherit behavior from global instructions.
</role_reminder>

<scope>
- Fetch data from external sources (HTTP APIs, async job polling, bulk downloads)
- Write raw bytes to `jackfruit-raw` bucket
- Do NOT parse/transform/normalize beyond what's needed for storage (decompression OK)
</scope>

<storage_contract>
Raw key pattern:
{source}/{dataset}/{variable}/ingest_date=YYYY-MM-DD/{filename}

Rules:
- Raw is append-only; NEVER overwrite existing objects
- Idempotency via `Exists()` check — skip if present
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
