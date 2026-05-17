FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        openjdk-11-jdk \
        wget \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Java config
RUN ln -s /usr/lib/jvm/java-11-openjdk-* /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

RUN wget https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz && \
    tar -xzf flink-1.17.2-bin-scala_2.12.tgz && \
    mv flink-1.17.2 /opt/flink && \
    rm flink-1.17.2-bin-scala_2.12.tgz

ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

RUN mkdir -p /opt/flink/log && \
    chown -R airflow: /opt/flink

RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar && \
    wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ENV FLINK_CLASSPATH=/opt/flink/lib/*

ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.net=ALL-UNNAMED"

USER airflow

RUN pip install --no-cache-dir \
    apache-flink==1.17.2 \
    psycopg2-binary \
    pandas \
    pyarrow \
    pyspark \
    sqlalchemy

ENV PYFLINK_CLIENT_EXECUTABLE=python3
