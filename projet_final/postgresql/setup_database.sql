CREATE SCHEMA IF NOT EXISTS source_data;
CREATE SCHEMA IF NOT EXISTS processed_zone;
CREATE SCHEMA IF NOT EXISTS data_warehouse;
 
CREATE TABLE IF NOT EXISTS processed_zone.dim_weather (
    temperature DOUBLE PRECISION,
    humidite INTEGER,
    vent_speed DOUBLE PRECISION,
    condition_meteo TEXT,
    meteo_timestamp TIMESTAMP(3),
    category_meteo TEXT CHECK (
        category_meteo IN ('Clair', 'Pluvieux', 'Orageux', 'Autre')
    ),
    heure INT CHECK (
        heure BETWEEN 0 AND 23
    ),
    jour_semaine INT
);