import json
from datetime import datetime
import requests

data_url = "https://api.openweathermap.org/data/2.5/weather?q=New York&APPID=a0d4bdfec20f89f96b4ddc97f8c8c4e7&units=metric"

raw_meteo_data = requests.get(data_url).json()
file_name="meteo_" +  datetime.now().strftime("%Y-%m-%d_%H") +  ".json"

with open('meteo_nyc/' + file_name, 'w') as file:
    json.dump(raw_meteo_data, file)

print('Extraction réalisé avec succès !')