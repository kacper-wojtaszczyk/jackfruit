# ADR 001: Grid Data Storage

## Status

Accepted

## Context

Jackfruit needs to store curated grid data (environmental variables like PM2.5, temperature, etc. at lat/lon coordinates over time) in a format that:

1. **Python transformation layer** can write to after decoding raw GRIB files
2. **Go serving layer** can query by coordinates without loading entire files into memory

### Options Considered

**Option A: GRIB2 Curated Files**

Initial approach — transform raw GRIB to curated GRIB2 files stored in S3.

- ✅ Familiar format, preserves metadata
- ✅ Efficient storage (compressed, gridded)
- ❌ Go support is flaky — `eccodes` requires CGO, pure-Go libraries are incomplete
- ❌ Point queries require loading file into memory or complex indexing
- ❌ Managing file-per-variable-per-timestamp creates many small files

**Option B: Custom Binary Format**

Design a custom binary format optimized for point queries with JSON sidecar for metadata.

- ✅ Full control over layout and query patterns
- ✅ No external dependencies
- ❌ Reinventing the wheel — storage engines solve this better
- ❌ Must build indexing, compression, query optimization from scratch
- ❌ Maintenance burden for a hobby project

**Option C: ClickHouse**

Store grid data as rows in ClickHouse: `(variable, timestamp, lat, lon, value, unit, catalog_id, inserted_at)`.

- ✅ SQL queries replace complex file parsing
- ✅ Built-in nearest-neighbor via `ORDER BY greatCircleDistance(...) LIMIT 1`
- ✅ Mature Go driver (`clickhouse-go/v2`) — pure Go, no CGO
- ✅ Mature Python driver (`clickhouse-connect`)
- ✅ Handles compression, indexing, partitioning automatically
- ✅ Good query performance for point lookups with proper schema design
- ⚠️ Row-per-point storage is less compact than gridded formats
- ⚠️ Adds operational complexity (another service to run)
- ⚠️ ClickHouse optimized for analytical queries (large result sets) — single-row lookups are not its sweet spot, but acceptable for our scale

## Decision

Use **ClickHouse** for curated grid data storage.

The operational overhead is acceptable given:
- We already run Postgres for metadata — adding ClickHouse is incremental
- Docker Compose handles local dev; managed ClickHouse exists for production
- Query simplicity (SQL) outweighs storage efficiency concerns at our scale

## Consequences

### Positive

- **Simpler serving layer**: SQL queries instead of GRIB parsing or custom binary readers
- **No CGO**: Pure Go driver eliminates eccodes/GRIB library pain
- **Standard tooling**: Can query data directly with `clickhouse-client` for debugging
- **Built-in optimization**: Partitioning, compression, and indexing handled by ClickHouse

### Negative

- **Storage overhead**: Row-per-point less efficient than gridded binary formats
- **Operational complexity**: Another stateful service to manage
- **Single-row query pattern**: Not ClickHouse's strength, but acceptable at our scale

### Mitigations

- **Abstraction layer**: Both Python (`GridStoreResource` abstract base class) and Go (interface) code depends on a `GridStore` abstraction, not ClickHouse directly. This makes future storage changes possible by swapping implementations.
- **Schema design**: Proper partitioning and sorting keys minimize query overhead.

## References

- Python abstraction: `src/pipeline_python/storage/grid_store.py` (GridStoreResource abstract base class)
- Python implementation: `src/pipeline_python/storage/clickhouse.py` (ClickHouseGridStore)
- Go abstraction: `internal/domain/store.go` (GridStore interface)
- Schema design guidelines: `docs/guides/tasks/01-clickhouse-setup.md`
