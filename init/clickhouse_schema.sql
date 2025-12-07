-- ClickHouse table for user action aggregations
CREATE DATABASE IF NOT EXISTS mydb;

USE mydb;

-- Table for storing aggregated user actions
CREATE TABLE IF NOT EXISTS user_aggregations
(
    user_id UInt64,
    window_start DateTime,
    window_end DateTime,
    total_actions UInt32,
    aggregation_type String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (user_id, aggregation_type, window_start, window_end)
SETTINGS index_granularity = 8192;

-- Materialized view for real-time aggregations (1-5 minute windows)
CREATE TABLE IF NOT EXISTS user_aggregations_realtime
(
    user_id UInt64,
    window_start DateTime,
    window_end DateTime,
    total_actions UInt32,
    aggregation_type String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (user_id, aggregation_type, window_start, window_end)
TTL window_start + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

