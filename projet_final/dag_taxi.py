import subprocess
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="taxi_dag",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["taxi", "batch"]
)
def taxi_dag():

    @task()
    def ingest_taxi_trips_data():
        subprocess.run(
            ["python", "/opt/airflow/dags/projet_final/ingestion/taxi_ingest.py"],
            check=True
        )

    @task()
    def transform_taxi_trips():
        subprocess.run(
            ["python", "/opt/airflow/dags/projet_final/transformation/taxi_transform.py"],
            check=True
        )

    @task()
    def finalize_taxi_load():
        subprocess.run(["echo", "Taxi batch terminé"])

    ingest_taxi_trips_data() >> transform_taxi_trips() >> finalize_taxi_load()


dag = taxi_dag()