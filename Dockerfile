FROM apache/airflow:2.6.1-python3.8
COPY airflow-requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /airflow-requirements.txt