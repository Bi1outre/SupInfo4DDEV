
  create view "mobility_and_meteo"."data_warehouse"."source_fact_taxi_trips__dbt_tmp"
    
    
  as (
    SELECT
    "VendorID",
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    "Categorie_distance",
    "RatecodeID",
    store_and_fwd_flag,
    "PULocationID",
    "DOLocationID",
    pickup_zone_id,
    payment_type,
    "Type_paiement_desc",
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    "Pourcentage_pourboire",
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    "Airport_fee",
    "Duration_minutes",
    pickup_hour,
    pickup_day_of_week
FROM "mobility_and_meteo"."processed_zone"."fact_taxi_trips"
  );