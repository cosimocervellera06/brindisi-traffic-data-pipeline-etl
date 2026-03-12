DROP TABLE IF EXISTS traffic_observations;
DROP TABLE IF EXISTS road_segments;
DROP TABLE IF EXISTS roads;
DROP TABLE IF EXISTS data_sources;

CREATE TABLE data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    schema_version INT NOT NULL,
    reference_date DATE,
    ingestion_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE roads (
    osm_id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    class VARCHAR(50)
);

CREATE TABLE road_segments (
    id VARCHAR(64) PRIMARY KEY, 
    osm_id BIGINT REFERENCES roads(osm_id) ON DELETE CASCADE,
    length NUMERIC(10,2),
    geom_start VARCHAR(100),
    geom_end VARCHAR(100),
    max_speed INT,
    is_bridge BOOLEAN DEFAULT FALSE,
    is_tunnel BOOLEAN DEFAULT FALSE
);

CREATE TABLE traffic_observations (
    measurement_id SERIAL PRIMARY KEY,
    segment_id VARCHAR(64) REFERENCES road_segments(id) ON DELETE CASCADE,
    source_id INT REFERENCES data_sources(id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    speed_kmh INT,
    travel_time_sec INT,
    day_phase VARCHAR(20)
);