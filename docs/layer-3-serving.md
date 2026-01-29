# Layer 3 — Serving

Expose query interfaces for client projects. Abstracts storage backend from consumers.

> **Implementation:** See [serving-go/README.md](../serving-go/README.md) for detailed API contract, architecture, and development guide.

## Responsibilities

- API contracts and versioning
- Request validation
- Response formatting
- Translating high-level queries to ClickHouse
- Lineage lookup from Postgres
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
- No CGO required (unlike previous eccodes approach)

**Grid Data:** ClickHouse via `clickhouse-go/v2`

**Why ClickHouse:**
- SQL queries replace complex GRIB parsing
- Built-in nearest-neighbor via `ORDER BY distance LIMIT 1`
- No file management or coordinate math needed
- Mature Go driver with connection pooling

## Data Source

1. **ClickHouse:** Query `grid_data` table for values at coordinates
2. **Metadata DB (Postgres):** Query `catalog.curated_data` joined with `catalog.raw_files` for lineage (optional)

The API contract stays the same regardless of storage backend.

### ClickHouse Query Pattern

**Point query (exact grid match):**
```sql
SELECT value
FROM grid_data
WHERE variable = 'pm2p5'
  AND source = 'cams'
  AND timestamp = '2025-03-11 14:00:00'
  AND lat = 52.25
  AND lon = 13.50
```

**Nearest-neighbor query (recommended):**
```sql
SELECT value, lat, lon
FROM grid_data
WHERE variable = 'pm2p5'
  AND source = 'cams'
  AND timestamp = '2025-03-11 14:00:00'
ORDER BY (lat - 52.52)*(lat - 52.52) + (lon - 13.40)*(lon - 13.40)
LIMIT 1
```

### Query Flow

1. Parse request → extract variable, coordinates, timestamp
2. Snap timestamp to source granularity (e.g., nearest hour for CAMS)
3. Query ClickHouse with nearest-neighbor pattern
4. Optionally query Postgres for lineage metadata
5. Return value

## Query Interface

REST API. Clients provide coordinates, timestamp, and variable names. The serving layer handles source selection, file lookup, and value extraction.

**Endpoints:**
```
GET /health                                       # 204 No Content (liveness)
GET /v1/environmental?lat=&lon=&time=&vars=       # Query environmental data
```

**Example request:**
```
GET /v1/environmental?lat=52.52&lon=13.40&time=2025-03-12T14:55:00Z&vars=pm2p5,temperature
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
    },
    {
      "name": "temperature",
      "value": 285.5,
      "unit": "K",
      "metadata": {
        "ref_timestamp": "2025-03-12T14:00:00Z",
        "raw_file_id": "01890c24-905b-7122-b170-b60814e6ee07",
        "source": "ads",
        "dataset": "reanalysis-era5-single-levels"
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
- [x] Grid data storage — ClickHouse (was: GRIB2 files + eccodes)
- [x] Postgres for lineage — query `catalog.curated_data` for raw file provenance
- [x] Source selection — hardcoded in Go code (CAMS for MVP)
- [x] Coordinate mapping — nearest neighbor via ClickHouse `ORDER BY distance LIMIT 1`
- [x] Response shape — per-variable metadata with lineage
- [x] Error handling — fail entire request if any variable missing

## Open Questions

- [ ] Timestamp snapping tolerance (TBD during implementation)
- [ ] Granularity source (hardcode vs Postgres lookup)
- [ ] Caching strategy (production)
- [ ] Include lineage in every response or make it optional?
