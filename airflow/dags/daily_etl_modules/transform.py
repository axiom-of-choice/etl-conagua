import pandas as pd
import datetime
import logging
logger = logging.getLogger(__name__)
from typing import Any

def generate_table_1(json_file: Any) -> pd.DataFrame:
    '''Functions that generates the table for exercise 2 pushing it to the XCOM backend with key table_1
    Args:
        path (str, optional): Path of the file in S3 to transform
    Raises:
        ValueError: If fails the execution just for handling purposes
    Returns:
        None
    '''
    logger.info('Generating first table')
    try:
        df = pd.DataFrame(json_file)
        df[['fecha', 'hora']] = df['dloc'].str.split('T', expand=True)
        df['fecha'] = pd.to_datetime(df['fecha'])
        df['hora'] = df['hora'].astype('int')
        table_1 = df[(df['fecha'] == datetime.date.today().isoformat()) & 
            (abs(df['hora'] - datetime.datetime.today().hour) < 3) & 
            (df['hora'] < datetime.datetime.today().hour)].groupby(['ides', 'idmun', 'nes', 'nmun'])[['tmax','tmin', 'prec']].mean()
        table_1 = table_1.reset_index() 
        logger.info('Table 1 succesfully generated')
        return table_1
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Table 1 failed')
        raise ValueError

def generate_table_2(df:pd.DataFrame, df1: pd.DataFrame) -> pd.DataFrame:
    '''Functions that generates the table for exercise 3 pushing it to the XCOM backend with key table_2

    Args:

    Raises:
        ValueError: If fails the execution just for handling purposes

    Returns:
        None
    '''
    logger.info('Generating table 2')
    try:
        data_mun = df1
        table_3 = pd.merge(left=df, right=data_mun, how='inner', left_on=['ides', 'idmun'], right_on=['Cve_Ent', 'Cve_Mun'])
        table_3.drop(['Cve_Ent', 'Cve_Mun'], axis=1, inplace=True)
        logger.info(msg='Table 2 successfuly generated')
        logger.info(table_3)
    except Exception as e:
        logger.error(msg='Table 2 failed')
        logger.exception(e)
        raise ValueError



## if __name__ == '__main__':
##     table_1 = generate_table_1()
##     print(table_1)
##     talbe_2 = generate_table_2(table_1)
##     print(talbe_2)