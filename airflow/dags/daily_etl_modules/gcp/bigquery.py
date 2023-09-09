from typing import Any, Optional
from .utils import BQ_PD_DATA_MAPPER
import logging


from google.cloud import bigquery
import pandas as pd
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
    
    def _enforce_dataframe_schema(self, schema: List[bigquery.SchemaField], dataframe: pd.DataFrame) -> bool:
        if len(schema) != len(dataframe.columns):
            self.logger.error(f'Length of schema ({len(schema)}) is different than length of dataframe ({len(dataframe.columns)})')
            raise ValueError('Schema validation failed')
        if set([column.name for column in schema]) == set(dataframe.columns):
            self.logger.info(f'Reordering dataframe to match schema')
            try:
                dataframe = self._reorder_dataframe(schema, dataframe)
            except Exception as e:
                self.logger.error(f'Error reordering dataframe: {e}')
                raise ValueError('Schema validation failed')
        for i,column in enumerate(schema):
            if BQ_PD_DATA_MAPPER[column.field_type] != dataframe.dtypes[i]:
                self.logger.warning(f'Column {column.name} in schema is of type {column.field_type} but column {dataframe.columns[i]} in dataframe is of type {dataframe.dtypes[i].name.upper()}')
                try:
                    self.logger.warning(f'Trying to cast column {dataframe.columns[i]} to type {BQ_PD_DATA_MAPPER[column.field_type]}')
                    dataframe[dataframe.columns[i]] = dataframe[dataframe.columns[i]].astype(BQ_PD_DATA_MAPPER[column.field_type])
                except Exception as e:
                    self.logger.error(f'Error casting column {dataframe.columns[i]} to type {BQ_PD_DATA_MAPPER[column.field_type]}: {e}')
                    raise ValueError('Schema validation failed')
        self.logger.info(f'Schema validation passed')
        self.logger.info(f'Final schema: {dataframe.dtypes}')
        return dataframe

    def _reorder_dataframe(self, schema: List[bigquery.SchemaField], dataframe: pd.DataFrame) -> pd.DataFrame:
        cols = [column.name for column in schema]
        dataframe = dataframe[cols]
        return dataframe
        
    def ingest_dataframe(self, data: Any, table_id: str, partition_field: Optional[str] = None, params : Optional[Dict[str,str]] = None) -> None:
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = False
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field, type_=bigquery.TimePartitioningType.DAY)
        origin_schema = self.client.get_table(self.dataset_id + '.' + table_id).schema
        self.logger.info(f'Ingesting dataframe to {self.dataset_id}.{table_id}')
        data = self._enforce_dataframe_schema(origin_schema, data)
        job = self.client.load_table_from_dataframe(data, self.dataset_id + '.' + table_id, job_config=job_config)
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
    bq.ingest_dataframe(pd.DataFrame({'test': [1,2,3], "asd":1}), 'conagua_data', params={'timeout': 1000})
    