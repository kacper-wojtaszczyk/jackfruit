CREATE DATABASE IF NOT EXISTS jackfruit;

-- Grid data storage for environmental variables.
-- Each row is a single (variable, timestamp, lat, lon) measurement.
-- Serves point queries: nearest grid point for a given variable + time.
--
-- catalog_id references catalog.grid_data.id in Postgres (lineage/metadata).
-- The serving layer queries CH for values, then optionally looks up
-- catalog_id in Postgres for lineage (source, dataset, raw file).
--
-- ReplacingMergeTree(inserted_at) deduplicates rows with the same sorting key
-- during background merges, keeping the row with the highest inserted_at.
-- Queries must use FINAL to see deduplicated results.
CREATE TABLE IF NOT EXISTS jackfruit.grid_data (
    variable     LowCardinality(String),
    timestamp    DateTime,
    lat          Float32,
    lon          Float32,
    value        Float32,
    unit         LowCardinality(String),
    catalog_id   UUID,
    inserted_at  DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (variable, timestamp, lat, lon)
