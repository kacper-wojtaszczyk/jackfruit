-- Initialize jackfruit schema and tables
CREATE SCHEMA IF NOT EXISTS catalog;

-- Raw files ingested from external sources
CREATE TABLE catalog.raw_files (
    id              UUID PRIMARY KEY,           -- run_id from ingestion (app-generated UUIDv7)
    source          TEXT NOT NULL,              -- e.g., 'ads'
    dataset         TEXT NOT NULL,              -- e.g., 'cams-europe-air-quality-forecasts-forecast'
    date            DATE NOT NULL,              -- partition date
    s3_key          TEXT NOT NULL UNIQUE,       -- full key in jackfruit-raw bucket
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Curated files derived from raw files
CREATE TABLE catalog.curated_files (
    id              UUID PRIMARY KEY,           -- app-generated UUIDv7
    raw_file_id     UUID NOT NULL REFERENCES catalog.raw_files(id),
    variable        TEXT NOT NULL,              -- e.g., 'pm2p5', 'pm10'
    source          TEXT NOT NULL,              -- e.g., 'cams'
    timestamp       TIMESTAMPTZ NOT NULL,       -- valid time of data
    s3_key          TEXT NOT NULL UNIQUE,       -- full key in jackfruit-curated bucket
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for serving layer queries (variable + timestamp lookups)
CREATE INDEX idx_curated_files_lookup ON catalog.curated_files (variable, timestamp);

-- Index for lineage queries (find all curated files from a raw file)
CREATE INDEX idx_curated_files_raw ON catalog.curated_files (raw_file_id);
