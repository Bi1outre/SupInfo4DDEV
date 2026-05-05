from datetime import datetime
from airflow.sdk import DAG, task

with DAG(dag_id="taxi_dag", start_date=datetime(2026, 5, 5), schedule=None) as dag:
    
    @task()
    def extract_taxi_trips_data():
        subprocess.run(["python", "ingestion/taxi_ingest.py"])
    
    @task()
    def transform_taxi_trips():
        subprocess.run(["echo", "Script manquant"]) # A remplacer avec le script de transformation
    
    @task()
    def model_taxi_trips():
        subprocess.run(["echo", "Script manquant"]) # A remplir avec le script de transformation

    extract_taxi_trips_data() >> transform_taxi_trips() >> model_taxi_trips()