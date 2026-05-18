# SupInfo4DDEV
## Data Development Final Project
 
Conception d’un pipeline de données modulaire avec traitement en temps
réel et en batch
 
## Objectif :
 
Vous êtes consultant data pour une entreprise à New York. Votre équipe est chargée de
construire un pipeline de données modulaire et évolutif capable d’ingérer, stocker,
transformer et modéliser des données de trajets en taxi ainsi que des conditions
météorologiques en temps réel à NYC. Le résultat final alimentera un tableau de bord
pour mieux comprendre la mobilité et l’impact de la météo.
 
---
 
## Questions
 
---
 
### Dbt / Analyse
 
#### Quels comportements de trajets observe-t-on selon les types de météo ?
 
#### A quelle heure observe-t-on le plus de clients à haute valeur ?
 
Pour répondre à la question, on a décidé de partir du principe qu'un client correspond à l'ensemble des passagers dans le taxi pour le trajet.
 
Requête SQL :
 
```sql
SELECT
    t.pickup_hour AS heure,
    COUNT(t.passenger_count) AS nombre_clients_haute_valeur
FROM data_warehouse.trip_enriched t
JOIN data_warehouse.high_value_customers h
    ON t.passenger_count = h.passenger_count
GROUP BY heure
ORDER BY nombre_clients_haute_valeur DESC;
```
 
Pour la période de janvier à mars 2026, l'heure où l'on observe le plus de clients à haute valeur est 18h, suivi de de 17 et 16h. Celle où l'on en observe le moins est 4h.
 
#### La météo influence-t-elle le comportement en matière de pourboires ?
Pour répondre, nous pouvons vérifier la catégorie météo ou le nombre de trajet est la plus grande.

Requête SQL :

```sql
SELECT     
    w.category_meteo,
COUNT(*) AS nb_trips 
FROM processed_zone.fact_taxi_trips t 
JOIN dim_weather w     
   ON t.pickup_hour = w.heure 
GROUP BY w.category_meteo 
ORDER BY nb_trips DESC;
```

Les Moment où la météo est Clair sont ceux où il y a le plus de trajets.


###

```sql
SELECT 
    category_meteo,
    COUNT(*) AS nb_trajets,
    ROUND(AVG("Pourcentage_pourboire")::numeric, 2) AS pourboire_moyen_pct,
    ROUND(
        PERCENTILE_CONT(0.5) 
        WITHIN GROUP (ORDER BY "Pourcentage_pourboire")::numeric, 
        2
    ) AS pourboire_median_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE "Pourcentage_pourboire" > 0) / COUNT(*),
        2
    ) AS taux_trajets_avec_pourboire_pct,
    ROUND(AVG(total_amount)::numeric, 2) AS montant_moyen
FROM data_warehouse.trip_enriched
WHERE "Pourcentage_pourboire" IS NOT NULL
GROUP BY category_meteo
ORDER BY pourboire_moyen_pct DESC;
```