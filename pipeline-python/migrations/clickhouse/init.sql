CREATE DATABASE IF NOT EXISTS jackfruit;

CREATE TABLE IF NOT EXISTS jackfruit.grid_data (
    variable     LowCardinality(String),
    timestamp    DateTime,
    lat          Float32,
    lon          Float32,
    value        Float32,
    unit         LowCardinality(String),
    catalog_id   UUID
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (variable, timestamp, lat, lon)
