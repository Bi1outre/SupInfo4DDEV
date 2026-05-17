import json
from datetime import datetime
import requests
import os


BASE_DIR = "/opt/airflow/dags/projet_final"

def main():
    OUTPUT_DIR = os.path.join(BASE_DIR, "output", "meteo_nyc")
    os.makedirs(OUTPUT_DIR, exist_ok=True)


    data_url = "https://api.openweathermap.org/data/2.5/weather?q=New York&APPID=a0d4bdfec20f89f96b4ddc97f8c8c4e7&units=metric"

    raw_meteo_data = requests.get(data_url).json()
    file_name = "meteo_" + datetime.now().strftime("%Y-%m-%d_%H") + ".json"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    with open(file_path, 'w') as file:
        json.dump(raw_meteo_data, file, indent=2)

    print(f'Extraction réalisée avec succès. Fichier enregistré dans: {file_path}')

if __name__ == "__main__":
    main()
