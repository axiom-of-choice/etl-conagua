import logging
logger = logging.getLogger(__name__)
import boto3
import os
from dotenv import load_dotenv
import gzip
from aws.s3 import S3_Connector

s3 = S3_Connector(os.environ['S3_ACCESS_KEY_ID'], os.environ['S3_SECRET_ACCESS_KEY'])

def load_to_s3(file: gzip.GzipFile, bucket: str = os.environ['S3_BUCKET'], key: str = 'HourlyForecast_MX.json.gz') -> None:
    s3.upload_s3(bucket=bucket, obj=file, key=key)
    
def load_table_1()

if __name__ == '__main__':
    load_dotenv()
    
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']
                      )
    from extract import extract_raw_file
    file = extract_raw_file()