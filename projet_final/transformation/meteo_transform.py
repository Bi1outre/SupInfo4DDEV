import logging
import os
import glob
from pyflink.table import EnvironmentSettings, TableEnvironment

#Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#Supprime tous les fichiers JSON du répertoire apres traitement
def delete_processed_json_files(directory):
    try:
        json_files = glob.glob(os.path.join(directory, '*.json'))
        for file in json_files:
            os.remove(file)
            logger.info(f"Fichier supprimé : {file}")
        if json_files:
            logger.info(f"{len(json_files)} fichiers JSON supprimés")
        else:
            logger.info("Aucun fichier JSON a supprimer")
    except Exception as e:
        logger.error(f"Erreur lors de la suppression des fichiers : {e}")

def main():
    logger.info("Démarrage de la transformation meteo avec Flink")

    #environnement Flink
    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)

    #table source pour stockant liste des JSON meteo
    logger.info("Configuration de la source de données : surveillance du dossier meteo_nyc")
    table_env.execute_sql("""
        CREATE TABLE source_meteo (
            content STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/airflow/dags/projet_final/ingestion/meteo_nyc',
            'format' = 'raw'
        )
    """)

    #extraction des champs pertinents et ajout des champs calculés
    logger.info("Extraction et transformation des données meteo")
    processed_table = table_env.sql_query("""
        SELECT
            CAST(JSON_VALUE(content, '$.main.temp') AS DOUBLE) AS temperature,
            CAST(JSON_VALUE(content, '$.main.humidity') AS INT) AS humidite,
            CAST(JSON_VALUE(content, '$.wind.speed') AS DOUBLE) AS vent_speed,
            JSON_VALUE(content, '$.weather[0].main') AS condition_meteo,
            TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(content, '$.dt') AS INT))) AS timestamp,
            CASE
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Clear' THEN 'Clair'
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Rain' THEN 'Pluvieux'
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Thunderstorm' THEN 'Orageux'
                ELSE 'Autre'
            END AS category_meteo,
            EXTRACT(HOUR FROM TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(content, '$.dt') AS INT)))) AS heure,
            EXTRACT(DOW FROM TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(content, '$.dt') AS INT)))) AS jour_semaine
        FROM source_meteo
    """)

    #configuration de la table de destination 
    logger.info("Configuration de la table de destination : dim_weather ")
    table_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS dim_weather (
            temperature DOUBLE,
            humidite INT,
            vent_speed DOUBLE,
            condition_meteo STRING,
            timestamp TIMESTAMP(3),
            category_meteo STRING,
            heure INT,
            jour_semaine INT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/mobility_and_meteo',
            'table-name' = 'dim_weather',
            'username' = 'postgres',
            'password' = 'postgres'
        )
    """)

    # insertion dans PostgreSQL
    logger.info("Insertion des données transformées dans dim_weather")
    processed_table.execute_insert('dim_weather').wait()

    # suppression des fichiers JSON après traitement réussi
    logger.info("Suppression des fichiers JSON traités")
    delete_processed_json_files('/opt/airflow/dags/projet_final/ingestion/meteo_nyc')

    logger.info("Transformation meteo terminée")

if __name__ == "__main__":
    main()