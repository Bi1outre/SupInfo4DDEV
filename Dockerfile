FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        default-jdk-headless \
        python3-dev \
        libssl-dev \
        libffi-dev \
        wget \
        curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

RUN mkdir -p /opt/airflow/jars && \
    chown -R airflow: /opt/airflow/jars

RUN wget -P /opt/airflow/jars https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar && \
    wget -P /opt/airflow/jars https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.net=ALL-UNNAMED"

USER airflow
RUN pip install --no-cache-dir \
    apache-flink==1.17.2 \
    pyspark==3.5.4 \
    psycopg2-binary \
    pandas \
    pyarrow \
    sqlalchemy

ENV PYFLINK_CLIENT_EXECUTABLE=python3
