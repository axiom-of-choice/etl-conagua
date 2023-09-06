import requests
import gzip
import os
from dotenv import load_dotenv
import logging
logger = logging.getLogger(__name__)
from daily_etl_modules.aws.s3 import S3_Connector

load_dotenv()

API_URL = os.environ['CONAGUA_API']
#from utils import logger, logger_verbose

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

if __name__ == '__main__':
    import logging
    from jsonformatter import JsonFormatter



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

    file = _extract_raw_file()
    print(type(file))