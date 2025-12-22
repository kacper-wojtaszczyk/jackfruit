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
- Decompression (zip → NetCDF/GRIB) — happens at ingestion
- Checksums
- Format detection for extension (`.nc` vs `.grib2`)

**Not allowed:**
- Variable extraction or filtering
- Unit conversion
- Temporal/spatial subsetting

Think of raw as **evidence**, not data.

## Object Key Convention

**Pattern:**
```
{source}/{dataset}/{type}/{YYYY-MM-DD}.{ext}
```

**Example:**
```
cds/cams-europe-air-quality-forecasts/analysis/2025-03-12.nc
cds/cams-europe-air-quality-forecasts/forecast/2025-03-12.nc
```

**Key decisions:**
- Date is **ingest date** (when we fetched it), not event date (which is inside the NetCDF)
- `type` is request type: `analysis` or `forecast`
- Static filename: always `{date}.{ext}` — path contains all metadata
- Extension determined from actual format after unzipping (`.nc`, `.grib2`)
- Re-ingesting same date overwrites previous file (idempotent)

**Rationale:**
- CDS API returns **multi-variable** NetCDF files (e.g., pm2.5 + pm10 together)
- Splitting by variable doesn't make sense — parallel ingestion hits same queue anyway
- Type-based organization reflects actual API request structure
- ETL can discover files deterministically: construct path from known dataset/type/date

## Immutability

- Raw storage is append-only
- ETL reads from raw, writes to curated — **never mutates raw**
- Can delete curated, re-run ETL, rebuild from raw

## Open Questions

- [ ] Retention policy — keep forever? Archive to Glacier?
- [ ] Compression — at ingestion or leave as-is?
- [ ] Versioning — S3 versioning or timestamp in filename?

