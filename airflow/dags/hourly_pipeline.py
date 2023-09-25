from airflow import DAG
import datetime
from datetime import timedelta
#from hourly_etl_modules.extract import _extract_raw_file
from airflow.models import DAG
from common.custom_operators.S3_BQ import S3ToBigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import os
import toml
from common.aws.s3 import S3_Connector
from common.gcp.bigquery import BigQueryConnector
import logging
import requests
import gzip
logger = logging.getLogger(__name__)
from dotenv import load_dotenv
load_dotenv()

API_URL = os.environ['CONAGUA_API']




def _extract_raw_file(url: str = API_URL) -> gzip.GzipFile:
    ''' Requests the endpoint and retrieve the file compressed

    Args:
        url (str): url of the endpoint. Defaults to "https://smn.conagua.gob.mx/tools/GUI/webservices/?method=1"

    Returns:
        gzip.GzipFile: Route of the compressed file
    '''
    try:
        logger.info(msg='Requesting endpoint')
        ftpstream = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Extract raw file failed')
        raise ValueError
    logger.info(msg='Request successful')
    return ftpstream.content

def extract_process(s3_client: S3_Connector, url: str = API_URL) -> None:
    ''' Requests the endpoint and uplaods the file to S3 bucket'''
    try:
        s3_client.upload_s3(bucket=os.environ['S3_BUCKET'], obj=_extract_raw_file(url), key='HourlyForecast_MX.gz')
    except Exception as e:
        logger.exception(e)



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

bucket = os.environ['S3_BUCKET']
queries = toml.load('queries.toml')
base_table_hourly_query = queries['base_table']['hourly_query']
query_hourly_aggregate = queries['aggregations']['example_query_hourly']
test = queries['aggregations']['test']
filename = 'HourlyForecast_MX.gz'


with DAG(
    'hourly_weather_dag',
    default_args=default_args,
    description='ETL for the weahter web service',
    schedule_interval='0 * * * *', catchup=False
) as dag:
    create_base_table = BigQueryExecuteQueryOperator(task_id='create_hourly_base_table', sql=base_table_hourly_query, use_legacy_sql=False, gcp_conn_id='GCP')
    upload_s3_task = S3CreateObjectOperator(s3_bucket=bucket,s3_key="{{ds}}/" + filename, task_id='upload_s3', data=_extract_raw_file(os.environ['CONAGUA_API_HOURLY']), aws_conn_id='AWS', replace=True)
    load_raw_table = S3ToBigQuery(s3_client=s3, bq_client=bq, bucket=bucket, bq_table='hourly_raw_table', filename=filename, task_id='load_raw_hourly_table')
    aggregate_examples = BigQueryExecuteQueryOperator(task_id='aggregate_hourly_examples', sql=query_hourly_aggregate, use_legacy_sql=False, gcp_conn_id='GCP',
                                                      destination_dataset_table=f"{bq_project}.{bq_dataset}.aggregated_hourly_examples", write_disposition='WRITE_TRUNCATE')
    
    create_base_table >> upload_s3_task >> load_raw_table >> aggregate_examples