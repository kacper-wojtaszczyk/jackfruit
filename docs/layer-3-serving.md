# Layer 3 — Serving

Expose query interfaces for client projects. Abstracts storage backend from consumers.

## Responsibilities

- API contracts and versioning
- Request validation
- Response formatting
- Translating high-level queries to metadata DB + storage
- Caching (if needed)

## Does NOT Do

- ETL or transformation
- Storage management
- Domain-specific scoring (that's client logic)

## Technology

**Language:** Go

**Why Go:**
- Pairs with ingestion layer (Go for I/O, Python for data)
- Excellent HTTP server performance
- Single binary deployment

## Data Source

1. **Metadata DB (Postgres):** Query for tile locations matching bbox/time range (future)
2. **Object Storage (S3/MinIO):** Fetch data files from curated bucket via direct key construction

The API contract stays the same regardless of storage backend.

### Curated Key Structure

Single-timestamp files with plain paths. Key depth depends on source granularity.

```
curated/{source}/{dataset}/{variable}/{year}/{month}/{day}/{hour}/data.grib2   # hourly
curated/{source}/{dataset}/{variable}/{year}/{month}/{day}/data.grib2          # daily
curated/{source}/{dataset}/{variable}/{year}/W{week}/data.grib2                # weekly
```

### Key Construction (Go)

```go
var sourceGranularity = map[string]time.Duration{
    "cams":   time.Hour,
    "era5":   time.Hour,
    "glofas": 24 * time.Hour,
}

// Hourly
key := fmt.Sprintf("curated/%s/%s/%s/%04d/%02d/%02d/%02d/data.grib2",
    source, dataset, variable, year, month, day, hour)

// Daily
key := fmt.Sprintf("curated/%s/%s/%s/%04d/%02d/%02d/data.grib2",
    source, dataset, variable, year, month, day)

// Weekly (ISO week)
year, week := timestamp.ISOWeek()
key := fmt.Sprintf("curated/%s/%s/%s/%04d/W%02d/data.grib2",
    source, dataset, variable, year, week)
```

### Query Flow

1. Parse request → extract source, variable, timestamp
2. Snap timestamp to source granularity (e.g., nearest hour for CAMS)
3. Construct curated key directly (no S3 LIST needed)
4. Fetch single GRIB file via S3 GET
5. Extract grid cell for requested location
6. Return value

## Query Interface

Clients ask high-level questions:

```
GET /v1/weather?location=52.52,13.40&time=2025-03-12T14:00:00Z
GET /v1/air-quality?bbox=10,50,15,55&start=2025-03-01&end=2025-03-07
GET /v1/metrics?location=berlin&metrics=temperature,pm25,humidity
```

Clients never:
- Know about S3 paths or Parquet files
- Write SQL or DuckDB queries
- Care if backend is curated bucket or ClickHouse

## Why a Serving Layer?

- Centralizes query logic
- Absorbs schema changes — clients see stable contract
- Enables caching, rate limiting, auth
- Clean separation: clients ask "weather for Berlin", not raw queries

## Open Questions

- [ ] API design (endpoints, request/response contracts)
- [ ] Caching strategy
- [ ] Error handling patterns
- [ ] GRIB file parsing/serving approach
