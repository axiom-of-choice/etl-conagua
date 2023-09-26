FROM apache/airflow:2.5.3
USER root
RUN apt-get update \
  && apt-get install -y git libpq-dev python3 python3-pip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ADD requirements.txt .
COPY ./bq_sa.json /opt/airflow/bq_sa.json
USER "${AIRFLOW_UID}:0"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"
RUN pip install -r requirements.txt
RUN pip install --upgrade cffi
RUN airflow db upgrade