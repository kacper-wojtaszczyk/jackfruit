# Layer 4 — Warehouse

> **Status: On-hold.** For MVP, the curated bucket serves as the queryable store. ClickHouse will be added when performance requires it.

Fast analytical queries over harmonized environmental data.

## Responsibilities

- Unified schema enforcement
- Indexing for efficient queries (time, location, metric type)
- Partitioning strategy
- Providing query interface to serving layer

## Does NOT Do

- Raw data ingestion
- Transformation logic
- Client-specific computations

## Technology

**Database:** ClickHouse (self-hosted)

**Why ClickHouse:**
- Columnar OLAP engine suited to time-series data
- Client-server model (Dagster writes, Go reads)
- Self-hostable, no SaaS dependency
- Production-proven at scale

## Relationship to Curated Bucket

Two storage tiers:

1. **`jackfruit-curated` bucket** — Parquet files, source-of-truth for processed data
2. **ClickHouse** — Loaded from curated bucket for fast queries

If ClickHouse needs rebuilding, reload from curated.

## MVP Approach

For MVP:
- Serving layer queries curated bucket directly (via DuckDB)
- ClickHouse added later when query performance requires it
- Curated Parquet + event-time partitioning makes future loading straightforward

## Open Questions

- [ ] Schema design
- [ ] Partitioning strategy (align with curated bucket)
- [ ] Indexing approach
- [ ] Retention / archival policy

