# serving-go

Go HTTP service for querying environmental data from the Jackfruit platform.

## Status

| Component | Status |
|-----------|--------|
| Health endpoint (`/health`) | ✅ Done |
| ClickHouse client | ⏳ Next |
| Postgres catalog integration | ⏳ Planned |
| Environmental endpoint (`/v1/environmental`) | ⏳ Planned |
| Error handling | ⏳ Planned |
| Docker/containerization | ⏳ Planned |

## Purpose

The serving layer exposes query interfaces for client applications. It abstracts the storage backend (ClickHouse + Postgres) from consumers, providing a stable API contract.

**Clients ask:** "What's the PM2.5 at (52.52, 13.40) at 14:55 UTC?"  
**Serving layer handles:** ClickHouse query for value, optional Postgres lookup for lineage.

> **MVP Scope:** Initial implementation uses air quality variables (`pm2p5`, `pm10`) from CAMS. The architecture is **variable-agnostic** — the same endpoint serves any gridded environmental data (temperature, humidity, vegetation indices) as datasets are added.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                           Client                                 │
│   GET /v1/environmental?lat=52.52&lon=13.40&time=...&vars=...    │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     serving-go                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ HTTP Router │→ │ Query Logic │→ │ Response Formatter  │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────┬────────────────┬──────────────────────────────────┘
          │                │
          ▼                ▼
   ┌─────────────┐  ┌─────────────┐
   │  Postgres   │  │ ClickHouse  │
   │  (lineage)  │  │ (grid data) │
   └─────────────┘  └─────────────┘
```

## Request Flow

1. Parse request → extract coordinates, timestamp, variable list
2. For each variable (in parallel):
   - Query ClickHouse for value at nearest grid point
   - Optionally query Postgres for lineage metadata
3. Aggregate results
4. Return JSON response

## API Contract

### Health Check

```
GET /health
```

**Response:** `204 No Content`

Simple liveness check for container orchestration.

### Environmental Data Query

```
GET /v1/environmental?lat={lat}&lon={lon}&time={timestamp}&vars={var1,var2}
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | Yes | Latitude (-90 to 90) |
| `lon` | float | Yes | Longitude (-180 to 180) |
| `time` | ISO8601 | Yes | Requested timestamp (UTC) |
| `vars` | string | Yes | Comma-separated variable names (e.g., `pm2p5,pm10,temperature`) |

**Success Response (200):**

```json
{
  "location": {
    "lat": 52.52,
    "lon": 13.40
  },
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

**Error Response (4xx/5xx):**

```json
{
  "error": "variable pm10 not found within time tolerance",
  "code": "VARIABLE_NOT_FOUND"
}
```

**Error Codes:**

| Code | HTTP Status | Description |
|------|-------------|-------------|
| `INVALID_REQUEST` | 400 | Missing/invalid parameters |
| `VARIABLE_NOT_FOUND` | 404 | No data for variable within time tolerance |
| `INTERNAL_ERROR` | 500 | ClickHouse/Postgres failure |

**Behavior:**
- Request fails entirely if any requested variable is not found
- No partial responses

## Technology

### Grid Storage Abstraction

The domain service depends on a `GridStore` interface, not ClickHouse directly:

```go
// internal/domain/store.go
type GridStore interface {
    GetValue(ctx context.Context, variable, source string, timestamp time.Time, lat, lon float32) (*GridValue, error)
    Close() error
}
```

This enables:
- Unit testing with mock `GridStore` (no ClickHouse needed)
- Future storage backend swaps without changing domain logic

See [ADR 001](../docs/ADR/001-grid-data-storage.md) for the storage decision.

### Grid Data: ClickHouse

**Library:** `github.com/ClickHouse/clickhouse-go/v2`

**Why ClickHouse:**
- SQL queries replace complex GRIB parsing
- Built-in nearest-neighbor via `ORDER BY distance LIMIT 1`
- No CGO dependencies (pure Go driver)
- Excellent query performance on grid data
- Connection pooling built-in

**Query pattern:**
```sql
SELECT value, lat, lon
FROM grid_data
WHERE variable = 'pm2p5'
  AND source = 'cams'
  AND timestamp = '2025-03-11 14:00:00'
ORDER BY greatCircleDistance(lat, lon, $4, $5)
LIMIT 1
```

### Source Selection

For MVP, source is hardcoded to CAMS in Go code. The serving layer is responsible for selecting the data source — clients only specify coordinates, timestamp, and variables.

### Coordinate Mapping

Nearest neighbor via ClickHouse query. The `ORDER BY distance LIMIT 1` pattern finds the closest grid cell to the requested coordinates.

### Timestamp Handling

- Snap to last available datapoint **before** requested timestamp
- Tolerance window: TBD during implementation (likely 2× source granularity)
- Response always includes `ref_timestamp` showing actual data time

## Database Access

### ClickHouse (Grid Data)

Primary data source for environmental values.

**Query pattern:**
```sql
SELECT value, lat, lon
FROM grid_data
WHERE variable = $1
  AND source = $2
  AND timestamp = $3
ORDER BY greatCircleDistance(lat, lon, $4, $5)
LIMIT 1
```

### Postgres (Lineage — Optional)

Queries `catalog.curated_data` joined with `catalog.raw_files` for lineage metadata.

**Example query pattern:**
```sql
SELECT 
  cd.timestamp,
  cd.variable,
  rf.id AS raw_file_id,
  rf.source,
  rf.dataset
FROM catalog.curated_data cd
JOIN catalog.raw_files rf ON cd.raw_file_id = rf.id
WHERE cd.variable = $1
  AND cd.timestamp = $2
LIMIT 1
```

> **Note:** Lineage lookup may be optional depending on performance requirements. TBD.

## Configuration

Environment variables:

```bash
# Server
PORT=8080                    # HTTP port (default: 8080)

# ClickHouse (grid data)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=...
CLICKHOUSE_DATABASE=jackfruit

# Postgres (lineage metadata)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=jackfruit
POSTGRES_PASSWORD=...
POSTGRES_DB=jackfruit
```

## Local Development

```bash
# Start infrastructure
docker-compose up -d postgres clickhouse

# Run server
go run ./cmd/serving

# Test health endpoint
curl -i http://localhost:8080/health

# Test environmental endpoint (MVP: air quality variables)
curl "http://localhost:8080/v1/environmental?lat=52.52&lon=13.40&time=2025-03-12T14:55:00Z&vars=pm2p5,pm10"
```

## Project Structure (Planned)

```
serving-go/
├── cmd/
│   └── serving/
│       └── main.go          # Entry point, server setup
├── internal/
│   ├── api/
│   │   ├── handler.go       # HTTP handlers
│   │   ├── request.go       # Request parsing/validation
│   │   └── response.go      # Response formatting
│   ├── clickhouse/
│   │   └── client.go        # ClickHouse queries for grid data
│   ├── catalog/
│   │   └── repository.go    # Postgres queries for lineage
│   ├── domain/
│   │   └── environmental.go # Business logic orchestration
│   └── config/
│       └── config.go        # Environment config
├── go.mod
├── go.sum
├── Dockerfile

└── README.md
```

