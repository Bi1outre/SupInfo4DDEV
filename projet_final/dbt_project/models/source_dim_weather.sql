SELECT
    temperature, 
    humidite, 
    vent_speed, 
    condition_meteo, 
    meteo_timestamp, 
    category_meteo, 
    heure, 
    jour_semaine 
FROM {{ source('dbt_meteo_mobility', 'dim_weather') }}