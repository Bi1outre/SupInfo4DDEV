FROM apache/airflow:2.7.3-python3.11

# dépendances nécessaires compilation
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir apache-flink
RUN pip install --no-cache-dir psycopg2-binary pandas pyarrow
RUN pip install --no-cache-dir sqlalchemy
RUN pip install --no-cache-dir pyspark
