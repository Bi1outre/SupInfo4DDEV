
  create view "mobility_and_meteo"."data_warehouse"."source_dim_weather__dbt_tmp"
    
    
  as (
    SELECT
    temperature, 
    humidite, 
    vent_speed, 
    condition_meteo, 
    meteo_timestamp, 
    category_meteo, 
    heure, 
    jour_semaine 
FROM "mobility_and_meteo"."processed_zone"."dim_weather"
  );