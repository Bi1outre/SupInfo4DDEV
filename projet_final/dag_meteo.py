from datetime import datetime
from airflow.sdk import DAG, task

with DAG(dag_id="meteo_dag", start_date=datetime(2026, 5, 5), schedule=None) as dag:
    
    @task()
    def extract_meteo_data():
        subprocess.run(["python", "ingestion/meteo_ingest.py"])
    
    @task()
    def transform_meteo():
        subprocess.run(["echo", "Script manquant"]) # A remplacer avec le script de transformation
    
    @task()
    def model_meteo():
        subprocess.run(["echo", "Script manquant"]) # A remplir avec le script de transformation

    extract_meteo_data() >> transform_meteo() >> model_meteo()