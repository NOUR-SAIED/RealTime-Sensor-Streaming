-- init-db.sql
CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id INT,
    temperature INT,
    humidity INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);