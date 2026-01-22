# serving-go

Go HTTP service for querying environmental data from the Jackfruit platform.

## Status

| Component | Status |
|-----------|--------|
| Health endpoint (`/health`) | ✅ Done |
| Postgres catalog integration | ⏳ Next |
| GRIB2 reading (eccodes) | ⏳ Planned |
| Air quality endpoint (`/v1/air-quality`) | ⏳ Planned |
| Error handling | ⏳ Planned |
| Docker/containerization | ⏳ Planned |

## Purpose

The serving layer exposes query interfaces for client applications. It abstracts the storage backend (S3 + Postgres) from consumers, providing a stable API contract.

**Clients ask:** "What's the PM2.5 at (52.52, 13.40) at 14:55 UTC?"  
**Serving layer handles:** Postgres lookup, S3 fetch, GRIB2 parsing, coordinate extraction.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
│   GET /v1/air-quality?lat=52.52&lon=13.40&time=...&vars=... │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     serving-go                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ HTTP Router │→ │ Query Logic │→ │ Response Formatter  │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                                  │
│         │                ▼                                  │
│         │        ┌───────────────┐                          │
│         │        │ GRIB2 Reader  │ (eccodes)                │
│         │        └───────────────┘                          │
│         │                │                                  │
└─────────┼────────────────┼──────────────────────────────────┘
          │                │
          ▼                ▼
   ┌─────────────┐  ┌─────────────┐
   │  Postgres   │  │    MinIO    │
   │  (catalog)  │  │  (curated)  │
   └─────────────┘  └─────────────┘
```

## Request Flow

1. Parse request → extract coordinates, timestamp, variable list
2. For each variable (in parallel):
   - Query `catalog.curated_files` for matching file within time tolerance
   - If not found → fail entire request
   - Fetch GRIB2 file from `jackfruit-curated` bucket via S3 key
   - Extract value at (lat, lon) using eccodes `GetDataAtPoint()`
   - Collect lineage metadata from joined `raw_files` table
3. Aggregate results
4. Return JSON response

## API Contract

### Health Check

```
GET /health
```

**Response:** `204 No Content`

Simple liveness check for container orchestration.

### Air Quality Query

```
GET /v1/air-quality?lat={lat}&lon={lon}&time={timestamp}&vars={var1,var2}
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `lat` | float | Yes | Latitude (-90 to 90) |
| `lon` | float | Yes | Longitude (-180 to 180) |
| `time` | ISO8601 | Yes | Requested timestamp (UTC) |
| `vars` | string | Yes | Comma-separated variable names (e.g., `pm2p5,pm10`) |

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
      "name": "pm10",
      "value": 39.34,
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
| `INTERNAL_ERROR` | 500 | Postgres/S3/GRIB read failure |

**Behavior:**
- Request fails entirely if any requested variable is not found
- No partial responses

## Technology

### GRIB2 Parsing: eccodes (CGO)

**Why eccodes:**
- ECMWF's official library — handles all GRIB2 variants including PDT 4.40
- Built-in `GetDataAtPoint(lat, lon)` — no manual grid math
- Same library family as Python pipeline conceptually
- CAMS data uses PDT 4.40 (Atmospheric Chemical Constituents) which pure Go libraries don't support

**Trade-off:** CGO complicates builds, but Docker handles this cleanly.

**macOS local dev:**
```bash
brew install eccodes
```

**Docker build:**
```dockerfile
# Build stage
FROM golang:1.23 AS builder
RUN apt-get update && apt-get install -y libeccodes-dev
# ... build ...

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libeccodes0
COPY --from=builder /app/serving /app/serving
```

### Source Selection

For MVP, source is hardcoded to CAMS in Go code. The serving layer is responsible for selecting the data source — clients only specify coordinates, timestamp, and variables.

### Coordinate Mapping

Nearest neighbor interpolation for MVP. GRIB2 stores gridded data; eccodes handles finding the closest grid cell to the requested coordinates.

### Timestamp Handling

- Snap to last available datapoint **before** requested timestamp
- Tolerance window: TBD during implementation (likely 2× source granularity)
- Response always includes `ref_timestamp` showing actual data time

## Database Access

Queries `catalog.curated_files` joined with `catalog.raw_files` for lineage.

**Example query pattern:**
```sql
SELECT 
  cf.s3_key,
  cf.timestamp,
  cf.variable,
  rf.id AS raw_file_id,
  rf.source,
  rf.dataset
FROM catalog.curated_files cf
JOIN catalog.raw_files rf ON cf.raw_file_id = rf.id
WHERE cf.variable = $1
  AND cf.timestamp <= $2
  -- AND cf.timestamp >= $2 - interval '2 hours'  -- tolerance TBD
ORDER BY cf.timestamp DESC
LIMIT 1
```

## Configuration

Environment variables:

```bash
# Server
PORT=8080                    # HTTP port (default: 8080)

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=jackfruit
POSTGRES_PASSWORD=...
POSTGRES_DB=jackfruit

# S3/MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_CURATED_BUCKET=jackfruit-curated
MINIO_USE_SSL=false
```

## Local Development

```bash
# Install eccodes (macOS)
brew install eccodes

# Start infrastructure
docker-compose up -d postgres minio

# Run server
go run ./cmd/serving

# Test health endpoint
curl -i http://localhost:8080/health

# Test air quality endpoint
curl "http://localhost:8080/v1/air-quality?lat=52.52&lon=13.40&time=2025-03-12T14:55:00Z&vars=pm2p5,pm10"
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
│   ├── catalog/
│   │   └── repository.go    # Postgres queries
│   ├── grib/
│   │   └── reader.go        # eccodes wrapper
│   ├── storage/
│   │   └── s3.go            # S3/MinIO client
│   └── config/
│       └── config.go        # Environment config
├── go.mod
├── go.sum
├── Dockerfile
├── README.md
├── ROADMAP-MVP.md
└── ROADMAP-PRODUCTION.md
```

## Roadmaps

- [MVP Roadmap](../pipeline-python/data/ROADMAP-MVP.md) — Working prototype for PM2.5/PM10
- [Production Roadmap](../pipeline-python/data/ROADMAP-PRODUCTION.md) — Full web service with observability
