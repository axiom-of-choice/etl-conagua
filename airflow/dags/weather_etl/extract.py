import requests
import gzip
import datetime
import json
import shutil
from .utils import logger, logger_verbose
#from utils import logger, logger_verbose

def extract_raw_file(url: str = "https://smn.conagua.gob.mx/webservices/?method=3") -> gzip.GzipFile:
    ''' Requests the endpoint and retrieve the file compressed

    Args:
        url (str): url of the endpoint. Defaults to "https://smn.conagua.gob.mx/webservices/?method=3"

    Returns:
        gzip.GzipFile: File containing the raw compressed data
    '''
    try:
        today =  datetime.datetime.today().isoformat()
        ftpstream = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        with open(f'./airflow/data/raw/HourlyForecast_MX_{today}.gz', 'wb') as file:
            file.write(ftpstream.content)
            logger.info(msg='Extract raw file successful')
            return f'./airflow/data/raw/HourlyForecast_MX_{today}.gz'
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Extract raw file failed')
        

def extract_json(filepath:str) -> json:
    '''Uncompress the data in gzip format and returns a json format

    Args:
        filepath (str): Path of the gzip file

    Returns:
        json: Json data format
    '''
    #today = datetime.date.today().isoformat()
    try:
        with gzip.open(filepath, 'rb') as f_in:
            with open(f'./airflow/data/intermediate/HourlyForecast_MX.json', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(msg='Extract json file successful')
        return 
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Extract json file failed')
        
@logger_verbose
def extract(url: str = "https://smn.conagua.gob.mx/webservices/?method=3") -> None:
    '''Wrapper that extracts data from endpoint and converts the gzip into json format 

    Args:
        url (_type_, optional): Endpoint url. Defaults to "https://smn.conagua.gob.mx/webservices/?method=3".

    Returns:
        json: Data in Json format
    '''
    try:
        logger.info('Starting extract process')
        path = extract_raw_file(url)
        extract_json(path)
    except:
        logger.error('Extract process failed')
        #logger.error(e)

# if __name__ == '__main__':
#     extract('asas')