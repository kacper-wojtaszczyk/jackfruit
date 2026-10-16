# Jackfruit

Reusable environmental-data platform. pipeline-python/ owns ingestion/transformation; serving-go/ owns the read API. Consult repository docs/ and ADRs for relevant contracts; do not assume saved architecture summaries describe current code.

- Keep ingestion, transformation, and serving separate. Raw object storage is immutable/append-only; processing must be deterministic and idempotent.
- No S3 LIST operations in the pipeline: construct keys from known metadata. Use the existing GridStore and resource abstractions rather than binding consumers to ClickHouse or SDK clients.
- Preserve units, timestamps, per-variable coordinates, and catalog/lineage semantics. Verify missing-data and catalog-failure behavior before changing it; do not introduce silent partial success.
- Python: use uv, existing type hints/domain models, and Dagster resources for external dependencies. From pipeline-python/: make test-unit; make test-integration needs real infrastructure.
- Go: use go.mod versions, existing interfaces, explicit errors, and context propagation. From serving-go/: make test-short for unit checks; make test includes infrastructure-dependent tests.
- Inspect the relevant Makefile/test markers before choosing checks. Do not start Docker services, materialize live pipelines, or access production data merely to validate a local change.
