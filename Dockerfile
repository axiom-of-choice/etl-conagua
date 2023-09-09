FROM apache/airflow:2.5.3
USER root
RUN apt-get update \
  && apt-get install -y git libpq-dev python3 python3-pip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ADD requirements.txt .
COPY .env .
COPY ./airflow/dags /opt/airflow/dags
COPY ./airflow/logs /opt/airflow/logs
COPY ./airflow/data /opt/airflow/data
COPY ./airflow/plugins /opt/airflow/plugins
COPY ./airflow/airflow.cfg /opt/airflow/airflow.cfg
COPY ./airflow/queries.toml /opt/airflow/queries.toml
COPY ./bq_sa.json /opt/airflow/bq_sa.json
USER "${AIRFLOW_UID}:0"
RUN pip install -r requirements.txt
RUN pip install --upgrade cffi
RUN airflow db upgrade