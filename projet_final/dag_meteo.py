import subprocess
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="meteo_dag",
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False,
    tags=["meteo", "streaming"]
)
def meteo_dag():

    @task()
    def ingest_meteo_data():
        subprocess.run(
            ["python", "/opt/airflow/dags/projet_final/ingestion/meteo_ingest.py"],
            check=True
        )

    @task()
    def transform_meteo_data():
        subprocess.run(
            ["python", "/opt/airflow/dags/projet_final/transformation/meteo_transform.py"],
            check=True
        )

    @task()
    def finalize_meteo_load():
        subprocess.run(["echo", "Météo traité et chargé"])

    ingest_meteo_data() >> transform_meteo_data() >> finalize_meteo_load()


dag = meteo_dag()