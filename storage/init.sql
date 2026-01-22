-- storage/init.sql
CREATE DATABASE IF NOT EXISTS events_db;

CREATE TABLE IF NOT EXISTS events_db.events (
    event_id String,
    user_id UInt32,
    event_type LowCardinality(String),
    product_id String,
    timestamp DateTime64(3, 'UTC'),
    session_id String,
    value Float32
) ENGINE = MergeTree()
ORDER BY (event_type, timestamp)
PARTITION BY toYYYYMM(timestamp);