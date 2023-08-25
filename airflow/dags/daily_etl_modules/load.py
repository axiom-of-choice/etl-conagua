import datetime
from utils import logger, logger_verbose
import gzip 
import boto3
from io import BytesIO
import shutil
import os
from dotenv import load_dotenv

@logger_verbose
def write_s3(path:str, **kwargs) -> int:
    '''Function that writes the files in the local storage

    Args:
        path (str): Path to the file be written 

    Raises:
        ValueError: If the function fails Just for handling  and debug purposes

    Returns:
        _type_: Exit Code [-1,1]
    '''
    logger.info("Performing writing into local directory")
    try:
        df = kwargs['ti'].xcom_pull(key=kwargs['key'], task_ids = kwargs['task_id'])
        partition = kwargs.get('partition',datetime.datetime.today().isoformat(timespec='minutes'),)
        df.to_csv(f'{path}_{partition}.csv', index=False)
        logger.info(f'Write table {path}_{partition}.csv successful')
        return 1
    except Exception as e:
        logger.error(e)
        logger.error(msg=f'Failed to write table {path}_{partition}.csv')
        raise ValueError
    return -1
#def load_postgres():

def upload_json_gz(s3client, bucket, key, obj, upload_date=datetime.datetime.today().date().isoformat()):
    ''' upload python dict into s3 bucket with gzip archive '''
    # inmem = io.BytesIO()
    # with gzip.GzipFile(fileobj=inmem, mode='wb') as fh:
    #     with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
    #         wrapper.write(json.dumps(obj, ensure_ascii=False, default=default))
    # inmem.seek(0)
    # if not compressed_fp:
    #     compressed_fp = BytesIO()
    # with gzip.GzipFile(fileobj=compressed_fp, mode='wb') as gz:
    #     shutil.copyfileobj(obj, gz)
    # compressed_fp.seek(0)
    s3client.put_object(Bucket=bucket, Body=obj, Key=key)

if __name__ == '__main__':
    load_dotenv()
    
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
                      aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY']
                      )
    from extract import extract_raw_file
    file = extract_raw_file()
    upload_json_gz(s3, os.environ['S3_BUCKET'], 'HourlyForecast_MX.gz', file)