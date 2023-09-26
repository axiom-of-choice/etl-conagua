import logging
import os
from functools import wraps
import datetime
# create logger
logger = logging.getLogger("logging application")
logger.setLevel(logging.DEBUG)
from pathlib import Path
from inspect import getargs
from dotenv import load_dotenv
import requests
import gzip
from common.aws.s3 import S3_Connector

API_URL = os.environ['CONAGUA_API']


BASE_PAHT = Path(__file__).resolve().parent

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logging.basicConfig(
    filename='airflow/logs/logs.log',
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

def logger_verbose(func):
    from datetime import datetime, timezone

    @wraps(func)
    def wrapper(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return wrapper

@logger_verbose
def remove_staging_files(paths:list = ['/opt/airflow/data/raw', '/opt/airflow/data/intermediate']):
    '''Removes all raw and intermediate files used to generate the final tables to avoid duplication

    Args:
        path (list, optional): List of paths to iterate over files and delete all files. Defaults to ['./data/raw', './data/intermediate']'.
    '''
    for path in paths:
        logger.info(f'Removing al files for {path}')
        for file in os.listdir(path) :
            os.remove(f'{path}/{file}')

def validate_date(date_text):
        try:
            datetime.date.fromisoformat(date_text)
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYY-MM-DD")


def allow_kwargs(func):
    argspec = getargs(func)
    # if the original allows kwargs then do nothing
    if  argspec.keywords:
        return func
    @wraps(func)
    def newfoo(*args, **kwargs):
        #print "newfoo called with args=%r kwargs=%r"%(args,kwargs)
        some_args = dict((k,kwargs[k]) for k in argspec.args if k in kwargs) 
        return func(*args, **some_args)
    return newfoo


# Extract funcs

def _extract_raw_file(url: str) -> gzip.GzipFile:
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

def extract_process(s3_client: S3_Connector, url: str) -> None:
    ''' Requests the endpoint and uplaods the file to S3 bucket'''
    try:
        s3_client.upload_s3(bucket=os.environ['S3_BUCKET'], obj=_extract_raw_file(url), key='HourlyForecast_MX.gz')
    except Exception as e:
        logger.exception(e)