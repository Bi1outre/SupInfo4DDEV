import logging
import os
import glob
from pyflink.table import EnvironmentSettings, TableEnvironment

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)

    config = table_env.get_config().get_configuration()

    config.set_string(
        "pipeline.jars",
        "file:///opt/airflow/jars/flink-connector-jdbc-3.1.2-1.17.jar;"
        "file:///opt/airflow/jars/postgresql-42.7.3.jar"
    )

    print("JARS =", config.get_string("pipeline.jars", "NOT SET"))

    logger.info("Configuration de la source de données")
    table_env.execute_sql("""
        CREATE TABLE source_meteo (
            content STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/airflow/dags/projet_final/ingestion/meteo_nyc',
            'format' = 'raw'
        )
    """)

    logger.info("Transformation des données meteo")
    processed_table = table_env.sql_query("""
    SELECT
        temperature,
        humidite,
        vent_speed,
        condition_meteo,
        meteo_timestamp,
        category_meteo,
        CAST(EXTRACT(HOUR FROM meteo_timestamp) AS INT) AS heure,
        CAST(EXTRACT(DOW FROM meteo_timestamp) AS INT) AS jour_semaine
    FROM (
        SELECT
            CAST(JSON_VALUE(content, '$.main.temp') AS DOUBLE) AS temperature,
            CAST(JSON_VALUE(content, '$.main.humidity') AS INT) AS humidite,
            CAST(JSON_VALUE(content, '$.wind.speed') AS DOUBLE) AS vent_speed,
            JSON_VALUE(content, '$.weather[0].main') AS condition_meteo,
            TO_TIMESTAMP(FROM_UNIXTIME(CAST(JSON_VALUE(content, '$.dt') AS INT))) AS meteo_timestamp,
            CASE
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Clear' THEN 'Clair'
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Rain' THEN 'Pluvieux'
                WHEN JSON_VALUE(content, '$.weather[0].main') = 'Thunderstorm' THEN 'Orageux'
                ELSE 'Autre'
            END AS category_meteo
        FROM source_meteo
    )

    """)

    logger.info("Configuration de la table dim_weather")
    table_env.execute_sql("""
        CREATE TABLE dim_weather (
            temperature DOUBLE,
            humidite INT,
            vent_speed DOUBLE,
            condition_meteo STRING,
            meteo_timestamp TIMESTAMP(3),
            category_meteo STRING,
            heure INT,
            jour_semaine INT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/mobility_and_meteo',
            'table-name' = 'processed_zone.dim_weather',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    logger.info("Insertion dans PostgreSQL")
    processed_table.execute_insert('dim_weather').wait()

    logger.info("Suppression des fichiers JSON")
    delete_processed_json_files('/opt/airflow/dags/projet_final/ingestion/meteo_nyc')

    logger.info("Transformation meteo terminée")

if __name__ == "__main__":
    main()
