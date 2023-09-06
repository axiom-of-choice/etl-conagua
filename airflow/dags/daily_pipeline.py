import logging
from jsonformatter import JsonFormatter
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
from airflow.models import DAG
from daily_etl_modules.aws.s3 import S3_Connector
from daily_etl_modules.gcp.bigquery import BigQueryConnector
import os
from daily_etl_modules.extract import extract_process
from daily_etl_modules.transform import generate_table_1, generate_table_2



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
bq = BigQueryConnector(os.environ['GCP_PROJECT'], os.environ['BQ_DATASET'])
bucket = os.environ['S3_BUCKET']
filename = 'HourlyForecast_MX.gz'


with DAG(
    'daily_pipeline_dag',
    default_args=default_args,
    description='ETL for the weather daily web service',
    schedule_interval='0 * * * *', catchup=False
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract_process, op_kwargs={'url': os.environ['CONAGUA_API'], 's3_client': s3})
    generate_table_1_task = PythonOperator(task_id = 'generate_table_1', python_callable=generate_table_1, op_kwargs={'json_file':s3.download_s3_json(bucket=bucket, key=filename, obj='HourlyForecast_MX.json')})
    
    #generate_table_2_task = PythonOperator(task_id = 'generate_table_2', python_callable=generate_table_2, op_kwargs={}).execute_callable()
    #load_table_1_task = PythonOperator(task_id = 'load_table_1', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_1/table_1','key':'table_1', 'task_id': 'generate_table_1'})
    #load_table_2_task = PythonOperator(task_id = 'load_table_2', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_2/table_2','key':'table_2', 'task_id': 'generate_table_2'})
    #load_table_2_current_task = PythonOperator(task_id = 'load_table_2_current', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_2/table_2', 'key': 'table_2', 'task_id':'generate_table_2', 'partition':'current'})
    #clean_xcom=PythonOperator(task_id='clean_xcom', python_callable=cleanup_xcom)
    
    extract_task  >> generate_table_1_task  #load_table_1_task >> clean_staging_zone_task
    #generate_table_2_task >> load_table_2_task >> load_table_2_current_task >> clean_staging_zone_task
    #clean_staging_zone_task >> clean_xcom

