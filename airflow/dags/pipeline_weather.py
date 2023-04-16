from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
from weather_etl.utils import remove_staging_files
from weather_etl.extract import extract
from weather_etl.transform import generate_table_1, generate_table_2
from weather_etl.load import write_local



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime.today(),
    'email': ['isadohergar@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@hourly',
}

with DAG(
    'weather_dag',
    default_args=default_args,
    description='ETL for the weahter web service'
) as dag:
    clean_staging_zone_task = PythonOperator(task_id = 'remove_staging_files', python_callable=remove_staging_files)
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    table_1_task = PythonOperator(task_id = 'table_1', python_callable=generate_table_1)
    table_2_task = PythonOperator(task_id = 'table_2', python_callable=generate_table_2)
    load_task = PythonOperator(task_id = 'load', python_callable = write_local)
    
    clean_staging_zone_task >> extract_task >> table_1_task >> table_2_task >> load_task