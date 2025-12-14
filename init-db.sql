-- init-db.sql (NOUVEAU SCHÉMA)
DROP TABLE IF EXISTS sensor_data;

CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    sensor_id INT,
    device_type VARCHAR(50),
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    co2_level DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    status VARCHAR(20)
        CHECK (status IN ('normal', 'warning', 'malfunction')),
    location_x DOUBLE PRECISION,
    location_y DOUBLE PRECISION
);


CREATE TABLE IF NOT EXISTS sensor_data_agg (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    sensor_id INT,
    avg_temp DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_co2 DOUBLE PRECISION,
    avg_battery DOUBLE PRECISION,
    warning_count INT,
    malfunction_count INT
);
