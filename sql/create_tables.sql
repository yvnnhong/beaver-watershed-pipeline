-- RDS PostgreSQL table definitions for beaver watershed pipeline
-- Run this once to set up your RDS database before running the Lambda

-- main table: one row per beaver sighting with matched water quality data
CREATE TABLE IF NOT EXISTS beaver_water_joined (
    id SERIAL PRIMARY KEY,
    species VARCHAR(100),
    decimal_latitude FLOAT,
    decimal_longitude FLOAT,
    year INT,
    month INT,
    day INT,
    state_province VARCHAR(100),
    country VARCHAR(100),
    nearest_station VARCHAR(200),   -- matched USGS monitoring station name
    station_lat FLOAT,
    station_lon FLOAT,
    distance_km FLOAT,              -- distance from beaver sighting to nearest station
    avg_dissolved_oxygen FLOAT,     -- average dissolved oxygen at that station (mg/L, healthy = above 6.0)
    created_at TIMESTAMP DEFAULT NOW()
);

-- index on state for fast filtering in Streamlit dashboard
CREATE INDEX IF NOT EXISTS idx_state ON beaver_water_joined(state_province);

-- index on nearest_station for fast groupby queries
CREATE INDEX IF NOT EXISTS idx_station ON beaver_water_joined(nearest_station);

-- index on coordinates for geographic queries
CREATE INDEX IF NOT EXISTS idx_coords ON beaver_water_joined(decimal_latitude, decimal_longitude);
