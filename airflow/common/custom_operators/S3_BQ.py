from typing import Any, Optional
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from common.aws.s3 import S3_Connector
from common.gcp.bigquery import BigQueryConnector
import pandas as pd
import datetime as dt

class S3ToBigQuery(BaseOperator):
    def __init__(self, s3_client: S3_Connector, bq_client:BigQueryConnector, bucket :str, bq_table: str, filename: str, file_prefix: Optional[str] = None, transformation: Optional[callable] = None, partition_field: Optional[str]='load_date',*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_client = s3_client
        self.bq_client = bq_client
        self.bucket = bucket
        self.filename = filename
        self.transformation = transformation
        self.file_prefix = file_prefix
        self.bq_table = bq_table
        self.partition_field = partition_field
        
    def execute(self, context: Context) -> Any:
        if self.file_prefix is not None:
            file = self.s3_client.download_s3_json(bucket=self.bucket, file_name=self.filename, partition_field=self.file_prefix)
        else:
            self.file_prefix = dt.datetime.today().date().isoformat()
            file = self.s3_client.download_s3_json(bucket=self.bucket, file_name=self.filename)
        if self.transformation is not None:
            transformed_file = self.transformation(file)
        else:
            transformed_file = pd.DataFrame(file)
        transformed_file['load_date'] = self.file_prefix
        self.bq_client.ingest_dataframe(data=transformed_file, table_id=self.bq_table, partition_field=self.partition_field)
            
        
        
        