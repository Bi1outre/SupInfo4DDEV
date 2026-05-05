import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

url = URL.create(
    drivername="postgresql+psycopg",
    username="postgres",
    password="postgres",
    host="localhost",
    port=5432,
    database="mobility_and_meteo"
)

engine = create_engine(url)

data_path = Path.cwd() / 'yellow_taxi_nyc'
file_name = "yellow_tripdata_2026-0"

print("Début de l'extraction...")

for index in range(3):
    file_path = data_path / (file_name + str(index+1) + '.parquet')
    print("Fichier : " + str(file_path))

    if not file_path.exists():
        print("Le fichier n'existe pas...")
        continue
    
    print('Extraction du fichier en cours... ')
    df = pd.read_parquet(file_path)

    df.to_sql(
        name="raw_taxi_trips",
        con=engine,
        schema="source_data",
        if_exists="append",
        index=False
    )

print('Extraction réalisé avec succès !')