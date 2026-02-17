CREATE SCHEMA IF NOT EXISTS catalog;

-- Raw files ingested from external sources (one row per ingestion run).
CREATE TABLE catalog.raw_files (
    id              UUID PRIMARY KEY,           -- run_id from ingestion (app-generated UUIDv7)
    source          TEXT NOT NULL,              -- e.g., 'ads'
    dataset         TEXT NOT NULL,              -- e.g., 'cams-europe-air-quality-forecasts-forecast'
    date            DATE NOT NULL,              -- partition date
    s3_key          TEXT NOT NULL UNIQUE,       -- full key in jackfruit-raw bucket
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Metadata and lineage for curated data stored in ClickHouse.
-- Each row represents one (variable, timestamp) grid written to ClickHouse.
-- ClickHouse grid_data rows reference this table via catalog_id = curated_data.id.
-- The serving layer uses catalog_id from CH to look up lineage here.
CREATE TABLE catalog.curated_data (
    id              UUID PRIMARY KEY,           -- app-generated UUIDv7, referenced by CH grid_data.catalog_id
    raw_file_id     UUID NOT NULL REFERENCES catalog.raw_files(id),
    variable        TEXT NOT NULL,              -- e.g., 'pm2p5', 'pm10'
    unit            TEXT NOT NULL,              -- e.g., 'kg/m³'
    timestamp       TIMESTAMPTZ NOT NULL,       -- valid time of data
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for lineage queries (find all curated data derived from a raw file)
CREATE INDEX idx_curated_data_raw ON catalog.curated_data (raw_file_id);
