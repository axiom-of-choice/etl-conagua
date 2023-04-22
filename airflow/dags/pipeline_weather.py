from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import datetime
from datetime import timedelta
from weather_etl.utils import remove_staging_files
from weather_etl.extract import extract
from weather_etl.transform import generate_table_1, generate_table_2
from weather_etl.load import write_local
from airflow.models import DAG
from airflow.utils.db import provide_session
from airflow.models import XCom


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
    'end_date': datetime.datetime(2023, 5, 16)
}


@provide_session
def cleanup_xcom(session=None):
    #dag_id = context["ti"]["dag_id"]
    num_rows_deleted = 0
    try:
        num_rows_deleted = session.query(XCom).delete()
        session.commit()
    except:
        session.rollback()
    print(f"Deleted {num_rows_deleted} XCom rows")

with DAG(
    'weather_dag',
    default_args=default_args,
    description='ETL for the weahter web service',
    schedule_interval='0 * * * *', catchup=False
) as dag:
    clean_staging_zone_task = PythonOperator(task_id = 'remove_staging_files', python_callable=remove_staging_files)
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    generate_table_1_task = PythonOperator(task_id = 'generate_table_1', python_callable=generate_table_1)
    generate_table_2_task = PythonOperator(task_id = 'generate_table_2', python_callable=generate_table_2)
    load_table_1_task = PythonOperator(task_id = 'load_table_1', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_1/table_1','key':'table_1', 'task_id': 'generate_table_1'})
    load_table_2_task = PythonOperator(task_id = 'load_table_2', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_2/table_2','key':'table_2', 'task_id': 'generate_table_2'})
    load_table_2_current_task = PythonOperator(task_id = 'load_table_2_current', python_callable = write_local, op_kwargs={'path':'/opt/airflow/data/processed/process_2/table_2', 'key': 'table_2', 'task_id':'generate_table_2', 'partition':'current'})
    clean_xcom=PythonOperator(task_id='clean_xcom', python_callable=cleanup_xcom)
    
    extract_task >> generate_table_1_task 
    generate_table_1_task >> load_table_1_task >> clean_staging_zone_task
    generate_table_1_task >> generate_table_2_task >> load_table_2_task >> load_table_2_current_task >> clean_staging_zone_task
    clean_staging_zone_task >> clean_xcom