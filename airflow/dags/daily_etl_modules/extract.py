import requests
import gzip
import datetime
import shutil
from utils import logger, logger_verbose
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.environ['CONAGUA_API']
#from utils import logger, logger_verbose

def extract_raw_file(url: str = API_URL) -> gzip.GzipFile:
    ''' Requests the endpoint and retrieve the file compressed

    Args:
        url (str): url of the endpoint. Defaults to "https://smn.conagua.gob.mx/webservices/?method=3"

    Returns:
        gzip.GzipFile: Route of the compressed file
    '''
    try:
        # today =  datetime.datetime.today().isoformat()
        ftpstream = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        # with open(f'/opt/airflow/data/raw/HourlyForecast_MX_{today}.gz', 'wb') as file:
        #     file.write(ftpstream.content)
        #     logger.info(msg='Extract raw file successful')
        #     return f'/opt/airflow/data/raw/HourlyForecast_MX_{today}.gz'
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Extract raw file failed')
        raise ValueError
        
    return ftpstream.content

def extract_json(filepath:str) -> None:
    '''Uncompress the data in gzip format and returns a json format

    Args:
        filepath (str): Path of the gzip file

    Returns:
        json: Json data format file
    '''
    #today = datetime.date.today().isoformat()
    try:
        with gzip.open(filepath, 'rb') as f_in:
            with open(f'/opt/airflow/data/intermediate/HourlyForecast_MX.json', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(msg='Extract json file successful')
        return 
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Extract json file failed')
        raise ValueError
        
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
        raise ValueError
        #logger.error(e)

# if __name__ == '__main__':
#     extract('asas')