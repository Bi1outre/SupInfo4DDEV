SELECT
    taxi_trips."VendorID",
    taxi_trips.tpep_pickup_datetime,
    taxi_trips.tpep_dropoff_datetime,
    taxi_trips.passenger_count,
    taxi_trips.trip_distance,
    taxi_trips."Categorie_distance",
    taxi_trips."Type_paiement_desc",
    taxi_trips."Pourcentage_pourboire",
    taxi_trips.total_amount,
    taxi_trips."Duration_minutes",
    taxi_trips.pickup_hour,
    taxi_trips.pickup_day_of_week,

    weather.category_meteo

FROM {{ ref('source_fact_taxi_trips') }} AS taxi_trips 
LEFT JOIN {{ ref('source_dim_weather') }} AS weather ON taxi_trips.pickup_hour = weather.heure