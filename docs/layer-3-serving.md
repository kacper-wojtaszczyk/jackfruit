# Layer 3 — Serving

Expose query interfaces for client projects. Abstracts storage backend from consumers.

> **Implementation:** See [serving-go/README.md](../serving-go/README.md) for detailed API contract, architecture, and development guide.

## Responsibilities

- API contracts and versioning
- Request validation
- Response formatting
- Translating high-level queries to metadata DB + storage
- Caching (production)

## Does NOT Do

- ETL or transformation
- Storage management
- Domain-specific scoring (that's client logic)

## Technology

**Language:** Go

**Why Go:**
- Excellent HTTP server performance and native concurrency
- Parallel variable fetching via goroutines
- Single binary deployment

**GRIB2 Parsing:** eccodes (CGO)

**Why eccodes:**
- ECMWF's official library — handles all GRIB2 variants including PDT 4.40
- Built-in `GetDataAtPoint(lat, lon)` — no manual grid math
- CAMS data uses PDT 4.40 (Atmospheric Chemical Constituents) which pure Go libraries don't support

## Data Source

1. **Metadata DB (Postgres):** Query `catalog.curated_files` joined with `catalog.raw_files` for file locations and lineage
2. **Object Storage (S3/MinIO):** Fetch GRIB2 files from curated bucket via S3 key from catalog

The API contract stays the same regardless of storage backend.

### Curated Key Structure

Single-timestamp files with plain paths. Key depth depends on source granularity.

```
{variable}/{source}/{year}/{month}/{day}/{hour}/data.grib2   # hourly
{variable}/{source}/{year}/{month}/{day}/data.grib2          # daily
{variable}/{source}/{year}/W{week}/data.grib2                # weekly
```

### Key Construction (Go)

```go
var sourceGranularity = map[string]time.Duration{
    "cams":   time.Hour,
    "era5":   time.Hour,
    "glofas": 24 * time.Hour,
}

// Hourly
key := fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d/data.grib2",
    variable, source, year, month, day, hour)

// Daily
key := fmt.Sprintf("%s/%s/%04d/%02d/%02d/data.grib2",
    variable, source, year, month, day)

// Weekly (ISO week)
year, week := timestamp.ISOWeek()
key := fmt.Sprintf("%s/%s/%04d/W%02d/data.grib2",
    variable, source, year, week)
```

### Query Flow

1. Parse request → extract variable, source, timestamp
2. Snap timestamp to source granularity (e.g., nearest hour for CAMS)
3. Construct curated key directly (no S3 LIST needed)
4. Fetch single GRIB file via S3 GET
5. Extract grid cell for requested location
6. Return value

## Query Interface

REST API. Clients provide coordinates, timestamp, and variable names. The serving layer handles source selection, file lookup, and value extraction.

**Endpoints:**
```
GET /health                           # 204 No Content (liveness)
GET /v1/air-quality?lat=&lon=&time=&vars=   # Query air quality data
```

**Example request:**
```
GET /v1/air-quality?lat=52.52&lon=13.40&time=2025-03-12T14:55:00Z&vars=pm2p5,pm10
```

**Example response:**
```json
{
  "location": { "lat": 52.52, "lon": 13.40 },
  "requested_timestamp": "2025-03-12T14:55:00Z",
  "variables": [
    {
      "name": "pm2p5",
      "value": 12.34,
      "unit": "µg/m³",
      "metadata": {
        "ref_timestamp": "2025-03-12T14:00:00Z",
        "raw_file_id": "01890c24-905b-7122-b170-b60814e6ee06",
        "source": "ads",
        "dataset": "cams-europe-air-quality-forecasts-analysis"
      }
    }
  ]
}
```

Clients never:
- Know about S3 paths or bucket structure
- Know about source selection logic
- Care if backend is curated bucket or ClickHouse

## Why a Serving Layer?

- Centralizes query logic
- Absorbs schema changes — clients see stable contract
- Enables caching, rate limiting, auth
- Clean separation: clients ask "weather for Berlin", not raw queries

## Decisions Made

- [x] API style — REST
- [x] GRIB parsing — eccodes (CGO)
- [x] Postgres for lookups — always query catalog, not direct key construction
- [x] Source selection — hardcoded in Go code (CAMS for MVP)
- [x] Coordinate mapping — nearest neighbor (MVP)
- [x] Response shape — per-variable metadata with lineage
- [x] Error handling — fail entire request if any variable missing

## Open Questions

- [ ] Timestamp snapping tolerance (TBD during implementation)
- [ ] Granularity source (hardcode vs Postgres lookup)
- [ ] Caching strategy (production)
