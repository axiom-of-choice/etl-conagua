import datetime
from dags.utils import logger_verbose, validate_date
from typing import Any, Optional
import os
import logging
import datetime


from google.cloud import bigquery
import pandas
import pytz
from typing import List

class BigQueryConnector(bigquery.Client):
    logger = logging.getLogger(__name__)
    def __init__(self, project_id: str, dataset_id: str, sa_location: Optional[str] = None) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        if sa_location is None:
            self.client = bigquery.Client(credentials=sa_location)
        self.client = bigquery.Client()
        self.sa_location = sa_location
        # self.dataset_ref = self.client.dataset(self.dataset_id)
        # self.table_ref = self.dataset_ref.table(self.table_id)
        # self.table = self.client.get_table(self.table_ref)
    def ingest_dataframe(self, data: Any, schema: List[bigquery.SchemaField], table_id: str, partition_date: Optional[str] = None, **kwargs) -> None:
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_date)
        job_config.schema = schema
        job = self.client.load_table_from_dataframe(data, self.dataset_id + '.' + table_id, job_config=job_config, **kwargs)


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    bq = BigQueryConnector('test', 'test')
    bq.ingest_dataframe(pandas.DataFrame({'test': [1,2,3]}), [{'name': 'test', 'type': 'INTEGER'}], 'test', partition_date='test', kwargs={'timeout': 1000})
    