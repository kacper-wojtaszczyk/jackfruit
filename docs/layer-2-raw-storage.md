# Layer 2 — Raw Storage

Store immutable, versioned raw data from ingestion. Source-of-truth for reproducibility.

## Responsibilities

- File organization and naming conventions
- Retention policy
- Versioning (if same data is re-ingested)
- Providing read access to ETL layer

## Does NOT Do

- Transformation or parsing
- Serving queries to end-users
- Schema enforcement

## Technology

**Dev:** MinIO  
**Prod:** S3

S3-compatible API means code works unchanged across environments.

## Bucket Strategy

Two separate buckets:

| Bucket | Purpose |
|--------|---------|
| `jackfruit-raw` | Immutable, append-only, source-faithful |
| `jackfruit-curated` | Processed, query-optimized (written by ETL) |

**Why separate buckets:**
- Clear separation of concerns
- Easier lifecycle rules (raw kept longer, curated maybe pruned)
- Safety — ETL bugs can't corrupt raw data

## What "Raw" Means

Raw does **not** mean "cleaned". Raw means:

- As close to the source as possible
- No semantic transformation
- No aggregation or resampling
- No merging across sources

**Allowed:**
- Decompression (zip → NetCDF/GRIB)
- Checksums
- Deterministic file renaming

Think of raw as **evidence**, not data.

## Object Key Convention

**Pattern:**
```
{source}/{dataset}/{variable}/ingest_date=YYYY-MM-DD/{filename}
```

**Example:**
```
cds/cams-europe-air-quality-forecasts/pm2p5/ingest_date=2025-03-12/pm2p5_20250311T1200.nc
```

**Key decisions:**
- `ingest_date` for traceability (curated layer partitions by event time)
- Variable in path for filtering
- Original filename or deterministic rename preserved

## Immutability

- Raw storage is append-only
- ETL reads from raw, writes to curated — **never mutates raw**
- Can delete curated, re-run ETL, rebuild from raw

## Open Questions

- [ ] Retention policy — keep forever? Archive to Glacier?
- [ ] Compression — at ingestion or leave as-is?
- [ ] Versioning — S3 versioning or timestamp in filename?

