import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, unix_timestamp, dayofweek, hour,
    round as spark_round
)

#logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Confi Spark
SPARK_CONFIG = {
    "master": "local[*]",
    "app_name": "TaxiTransformation",
    "driver_memory": "2G",
    "log_level": "WARN",
}

OUTPUT_TABLE = "processed_zone.fact_taxi_trips"

# Session Spark
def get_spark_session(config):
    spark = (
        SparkSession.builder
        .master(config["master"])
        .appName(config["app_name"])
        .config("spark.driver.memory", config["driver_memory"])
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config["log_level"])
    return spark

# Lecture des données source
def read_source_data(spark):
    logger.info("Lecture des données taxi brutes depuis les fichiers Parquet")

    # Chemins des fichiers
    data_paths = [
        "/opt/airflow/dags/projet_final/ingestion/yellow_taxi_nyc/yellow_tripdata_2026-01.parquet",
        "/opt/airflow/dags/projet_final/ingestion/yellow_taxi_nyc/yellow_tripdata_2026-02.parquet",
        "/opt/airflow/dags/projet_final/ingestion/yellow_taxi_nyc/yellow_tripdata_2026-03.parquet"
    ]

    df = spark.read.parquet(*data_paths)
    logger.info(f"✓ {df.count()} lignes lues")
    return df

# Transformation des données
def transform_data(df):
    logger.info("Transformation des données taxi")

    # Durée Trajet
    df = df.withColumn(
        "Duration_minutes",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60,
            2
        )
    )

    # Tranches distance
    df = df.withColumn(
        "Categorie_distance",
        when(col("trip_distance") <= 2, "0-2 km")
        .when((col("trip_distance") > 2) & (col("trip_distance") <= 5), "2-5 km")
        .otherwise(">5 km")
    )

    # Paiement
    df = df.withColumn(
        "Type_paiement_desc",
        when(col("payment_type") == 1, "Carte de crédit")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "Gratuit")
        .when(col("payment_type") == 4, "Litige")
        .when(col("payment_type") == 5, "Inconnu")
        .when(col("payment_type") == 6, "Trip annulé")
        .otherwise("Inconnu")
    )

    # Pourcentage de pourboire
    df = df.withColumn(
        "Pourcentage_pourboire",
        spark_round(
            when(col("fare_amount") > 0, col("tip_amount") / col("fare_amount") * 100)
            .otherwise(0),
            2
        )
    )

    # Heure de prise en charge
    df = df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))

    # Jour de la semaine (1=Lundi, 7=Dimanche)
    df = df.withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))

    # Informations de zone (simplifié)
    df = df.withColumn("pickup_zone_id", col("PULocationID"))

    # Sélection des colonnes finales
    final_df = df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "Categorie_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "pickup_zone_id",
        "payment_type",
        "Type_paiement_desc",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "Pourcentage_pourboire",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "Duration_minutes",
        "pickup_hour",
        "pickup_day_of_week"
    )

    logger.info(f"✓ Transformation terminée, {final_df.count()} lignes")
    return final_df

# Sauvegarde Postgres
def save_to_postgres(df, spark):
    logger.info(f"Sauvegarde vers {OUTPUT_TABLE}")

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/mobility_and_meteo") \
        .option("dbtable", OUTPUT_TABLE) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    logger.info("✓ Données sauvegardées")


def main():
    logger.info("Début de la transformation taxi")

    spark = get_spark_session(SPARK_CONFIG)

    try:
        raw_df = read_source_data(spark)

        transformed_df = transform_data(raw_df)

        save_to_postgres(transformed_df, spark)

        logger.info("✓ Transformation taxi terminée avec succès")

    except Exception as e:
        logger.error(f"Erreur lors de la transformation: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()