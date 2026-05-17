import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DATA_DIR = "/opt/airflow/dags/projet_final/ingestion/yellow_taxi_nyc"


def main():
    data_path = Path(DATA_DIR)

    url = URL.create(
        drivername="postgresql+psycopg2",
        username="postgres",
        password="postgres",
        host="postgres",
        port=5432,
        database="mobility_and_meteo"
    )

    engine = create_engine(url)

    file_prefix = "yellow_tripdata_2026-0"
    files_to_process = [f"{file_prefix}{i+1}.parquet" for i in range(3)]

    for file_name in files_to_process:
        file_path = data_path / file_name
        print(f"Traitement du fichier: {file_path}")

        if not file_path.exists():
            print(f"Le fichier {file_name} n'existe pas, passage au suivant...")
            continue

        try:
            print(f"Extraction du fichier {file_name} en cours...")
            df = pd.read_parquet(file_path)

            if df.empty:
                print(f"Le fichier {file_name} est vide, passage au suivant...")
                continue

            df.to_sql(
                name="raw_taxi_trips",
                con=engine,
                schema="source_data",
                if_exists="append",
                index=False,
                chunksize=10000
            )

            print(f"Fichier {file_name} traité avec succès")

        except Exception as e:
            print(f"Erreur lors du traitement de {file_name}: {str(e)}")
            raise

    print("Extraction réalisée avec succès !")


if __name__ == "__main__":
    main()
