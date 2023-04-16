import pandas as pd
import datetime
from .utils import logger, logger_verbose

@logger_verbose
def write_local(path:str, **kwargs):
    '''_summary_

    Args:
        path (str): _description_
        df (pd.DataFrame): _description_
        partition (_type_, optional): _description_. Defaults to datetime.datetime.today().isoformat().
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
#def load_postgres():
    