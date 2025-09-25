CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_hourly
(
    ts DateTime,
    city_id UInt16,
    city LowCardinality(String),
    lat Float64,
    lon Float64,
    temp_c Float32,
    rel_humidity Float32,
    wind_speed_ms Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (city_id, ts);
