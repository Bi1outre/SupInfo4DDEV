SELECT 
    COUNT(*) AS nombre_trajet_heure,
    pickup_hour AS heure,
    category_meteo AS categorie_meteo,
    ROUND(AVG("Duration_minutes")::numeric, 2) AS duree_moyenne,
    ROUND(AVG("Pourcentage_pourboire")::numeric, 2) AS pourboire_moyen
FROM {{ ref('trip_enriched') }}
GROUP BY pickup_hour, category_meteo