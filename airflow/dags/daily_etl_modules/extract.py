import requests
import gzip
import boto3
import shutil
import os
from dotenv import load_dotenv
from typing import Any, Optional
import logging
logger = logging.getLogger(__name__)

load_dotenv()

API_URL = os.environ['CONAGUA_API']
#from utils import logger, logger_verbose

def extract_raw_file(url: str = API_URL) -> gzip.GzipFile:
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

    file = extract_raw_file()
    print(type(file))