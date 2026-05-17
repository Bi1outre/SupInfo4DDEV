FROM apache/airflow:2.7.3-python3.11

# dépendances nécessaires compilation
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*


USER airflow

RUN pip install --no-cache-dir apache-flink
RUN pip install --no-cache-dir psycopg2-binary pandas pyarrow
RUN pip install --no-cache-dir sqlalchemy
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir java
