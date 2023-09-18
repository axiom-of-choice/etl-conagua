import datetime
import logging
logger = logging.getLogger(__name__)


def write_local(path:str, **kwargs) -> int:
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
    