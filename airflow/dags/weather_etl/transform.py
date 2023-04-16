import pandas as pd
import datetime
from .utils import logger, logger_verbose
import os

@logger_verbose
def generate_table_1(path: str = './airflow/data/intermediate/HourlyForecast_MX.json') -> pd.DataFrame:
    '''Generates first table that contains the mean of temperature and precipitation aggregated by state and county 

    Args:
        path (str): Path of the json file

    Returns:
        pd.DataFrame: Table
    '''
    logger.info('Generating first table')
    try:
        df = pd.read_json(path)
        df[['fecha', 'hora']] = df['hloc'].str.split('T', expand=True)
        df['fecha'] = pd.to_datetime(df['fecha'])
        df['hora'] = df['hora'].astype('int')
        table_1 = df[(df['fecha'] == datetime.date.today().isoformat()) & 
            (abs(df['hora'] - datetime.datetime.today().hour) < 3) & 
            (df['hora'] < datetime.datetime.today().hour)].groupby(['ides', 'idmun'])[['temp', 'prec']].mean()
        table_1 = table_1.reset_index() 
        logger.info('Table 1 succesfully generated')
        return table_1
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Table 1 failed')

@logger_verbose
def generate_table_2(df: pd.DataFrame, path:str = './airflow/data/data_municipios') -> pd.DataFrame:
    '''Generates the joined table between aggregated table and local tables

    Args:
        df (pd.DataFrame): Aggregated table of step 1
        path (str, optional): Path were local tables are stored. Defaults to './data/data_municipios'.

    Returns:
        pd.DataFrame: Joined table
    '''
    logger.info('Generating table 2')
    try:
        latest_data_path = os.listdir(path)[0]
        # data_mun_1 = pd.read_csv(path + '/data.csv')
        # data_mun_2 = pd.read_csv(path + '/data_1.csv')
        # data_mun = pd.concat([data_mun_1,data_mun_2])
        # del(data_mun_1)
        # del(data_mun_2)
        data_mun = pd.read_csv(f'{path}/{latest_data_path}')
        table_3 = pd.merge(left=df, right=data_mun, how='inner', left_on=['ides', 'idmun'], right_on=['Cve_Ent', 'Cve_Mun'])
        table_3.drop(['Cve_Ent', 'Cve_Mun'], axis=1, inplace=True)
        logger.info(msg='Table 2 successfuly generated')
        return table_3
    except Exception as e:
        logger.exception(e)
        logger.error(msg='Table 2 failed')

# def transform(path: str = './data/intermediate/HourlyForecast_MX.json') -> pd.DataFrame:
#     '''Wrapper that generates both tables
# 
#     Args:
#         path (str, optional): Path where json data staging is stored. Defaults to './data/raw/HourlyForecast_MX.json'.
#     Returns:
#         pd.DataFrame: Both dataframes. Table1, table2
#     '''
#     logging.info("Performing extract")
#     table_1 = generate_table_1(path)
#     table_2 = generate_table_2(table_1)
#     return table_1, table_2

## if __name__ == '__main__':
##     table_1 = generate_table_1()
##     print(table_1)
##     talbe_2 = generate_table_2(table_1)
##     print(talbe_2)