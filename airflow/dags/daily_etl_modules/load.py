import datetime
from utils import logger, logger_verbose
import boto3
import os
from dotenv import load_dotenv

if __name__ == '__main__':
    load_dotenv()
    
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']
                      )
    from extract import extract_raw_file
    file = extract_raw_file()