import datetime
from dags.utils import logger_verbose, validate_date
from typing import Any, Optional
import os
import logging
import datetime


from google.cloud import bigquery
import pandas
import pytz
from typing import List, Dict, Any, Optional

class BigQueryConnector(bigquery.Client):
    logger = logging.getLogger(__name__)
    def __init__(self, project_id: str, dataset_id: str, sa_location: Optional[str] = None) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        if sa_location is None:
            self.client = bigquery.Client(project=project_id)
        self.client = bigquery.Client(project=project_id, credentials=sa_location)
        self.sa_location = sa_location
        
        
    def ingest_dataframe(self, data: Any, schema: List[bigquery.SchemaField], table_id: str, partition_date: Optional[str] = None, params : Optional[Dict[str,str]] = None) -> None:
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = False
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_date)
        job_config.schema = schema
        origin_schema = self.client.get_table(self.dataset_id + '.' + table_id).schema
        logger.info(f'Origin schema: {origin_schema}')
        self.logger.info(f'Ingesting dataframe to {self.dataset_id}.{table_id}')
        job = self.client.load_table_from_dataframe(data, self.dataset_id + '.' + table_id, job_config=job_config, **params)
        job.result()
        self.logger.info(f'Ingestion of {len(data)} rows to {self.dataset_id}.{table_id} completed')


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
    from dotenv import load_dotenv
    load_dotenv()
    bq = BigQueryConnector('orbital-craft-397002', 'conagua_data')
    bq.ingest_dataframe(pandas.DataFrame({'test': [1,2,3], "asd":"asd"}), [{'name': 'test', 'type': 'INTEGER'}], 'conagua_data', params={'timeout': 1000})
    