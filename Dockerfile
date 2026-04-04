FROM apache/airflow:2.10.3-python3.11

USER airflow
RUN pip install --no-cache-dir \
    clickhouse-driver==0.2.7 \
    confluent-kafka==2.3.0