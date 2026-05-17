SELECT
    passenger_count,
    COUNT(*) AS nombre_trajet,
    ROUND(SUM(total_amount)) AS total_depense,
    ROUND(AVG("Pourcentage_pourboire"::numeric), 2) AS moyenne_pourcentage_pourboire
FROM {{ ref('trip_enriched')}}
GROUP BY passenger_count
HAVING COUNT(*) > 10 AND SUM(total_amount) > 300 AND AVG("Pourcentage_pourboire") > 15