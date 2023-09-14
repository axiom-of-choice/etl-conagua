import logging
from jsonformatter import JsonFormatter
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3Hook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from custom_operators.S3_BQ import S3ToBigQuery
from daily_etl_modules.aws.s3 import S3_Connector
from daily_etl_modules.gcp.bigquery import BigQueryConnector
import os
from daily_etl_modules.extract import _extract_raw_file, extract_process
from daily_etl_modules.transform import generate_table_1, generate_table_2
from dotenv import load_dotenv
load_dotenv()
import toml



STRING_FORMAT = '''{
    "Name":            "name",
    "Levelname":       "levelname",
    "FuncName":        "funcName",
    "Timestamp":       "asctime",
    "Message":         "message"
}'''

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = JsonFormatter(STRING_FORMAT)

logHandler = logging.StreamHandler()
logHandler.setFormatter(formatter)
logHandler.setLevel(logging.INFO)

logger.addHandler(logHandler)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 4, 16, 3,8,0,0),
    'email': ['isadohergar@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '*/5 * * * *',
    'provide_context': True,
    'catchup': False,
    #'end_date': datetime.datetime(2023, 10, 16) Never ends
}

s3 = S3_Connector(os.environ['S3_ACCESS_KEY_ID'], os.environ['S3_SECRET_ACCESS_KEY'])
bq_project = os.environ['GCP_PROJECT']
bq_dataset = os.environ['BQ_DATASET']
bq = BigQueryConnector(os.environ['GCP_PROJECT'], os.environ['BQ_DATASET'])
bucket = os.environ['S3_BUCKET']

###TO-DO: Remove HARCODED 
filename = 'DailyForecast_MX.gz'
####
queries = toml.load("queries.toml")
baste_table_query = queries['base_table']['creation_query']
query_aggregate = queries['aggregations']['example_query']


with DAG(
    'daily_forecast_dag',
    default_args=default_args,
    description='ETL for the weather daily web service',
    schedule_interval='0 * * * *', catchup=False
) as dag_test:
    # aggregate_examples = BigQueryExecuteQueryOperator(task_id='aggregate_examples', sql=query, use_legacy_sql=False, gcp_conn_id='GCP',
    #                                                   destination_dataset_table=f"{bq_project}.{bq_dataset}.aggregated_examples", write_disposition='WRITE_TRUNCATE')
    create_base_table = BigQueryExecuteQueryOperator(task_id='create__base_table', sql=baste_table_query, use_legacy_sql=False, gcp_conn_id='GCP')
    upload_s3_task = S3CreateObjectOperator(s3_bucket=bucket,s3_key="{{ds}}/" + filename, task_id='upload_s3', data=_extract_raw_file(os.environ['CONAGUA_API']), aws_conn_id='AWS', replace=True)
    load_raw_table = S3ToBigQuery(s3_client=s3, bq_client=bq, bucket=bucket, bq_table='raw_daily_table', filename=filename, task_id='load_raw_table')
    aggregate_examples = BigQueryExecuteQueryOperator(task_id='aggregate_examples', sql=query_aggregate, use_legacy_sql=False, gcp_conn_id='GCP',
                                                      destination_dataset_table=f"{bq_project}.{bq_dataset}.aggregated_examples", write_disposition='WRITE_TRUNCATE')
    
    create_base_table >> upload_s3_task >> load_raw_table >> aggregate_examples