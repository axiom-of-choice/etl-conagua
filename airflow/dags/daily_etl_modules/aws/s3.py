#from utils import logger_verbose, validate_date
from typing import Any, Optional
import os
import boto3
import logging
import datetime
today = datetime.datetime.today().date().isoformat()
import json

class S3_Connector:
    logger = logging.getLogger(__name__)
    def __init__(self, access_key, secret_key) -> None:
        self.access_key = access_key
        self.secret_key = secret_key
        self.s3_client = boto3.client('s3', aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
    
    def download_s3_json(self, partition_date : Optional[str] = today, bucket: str = None, file_name: str = "HourlyForecast_MX.gz") -> Any :
        """Download the file from s3

        Args:
            s3_client (boto3.client): Client of s3
            partition_date (str, optional): Date of partition (if exists). In format "YYYY-MM-DD". Defaults to None.
            bucket (str, optional): Bucket name. Defaults to os.environ["S3_BUCKET"].
            file_name (str, optional): Name of the file. Defaults to "HourlyForecast_MX.gz".
        Returns:
            Any: File downloaded
        """
        if partition_date is not None:
            #validate_date(partition_date)
            self.logger.info(f'Downloading file {file_name} from s3 bucket {bucket} with partition date {partition_date}')
            s3_response_object = self.s3_client.get_object(Bucket=bucket, Key=f'{partition_date}/{file_name}')
            object_content = s3_response_object['Body'].read()
        else:
            self.logger.info(f'Downloading file {file_name} from s3 bucket {bucket}')
            s3_response_object = self.s3_client.get_object(Bucket=bucket, Key=f'{file_name}')
            object_content = s3_response_object['Body'].read()
        object_content = json.loads(json.loads(object_content.decode('utf-8')))
        return object_content
    
    def upload_s3(self, bucket: str, obj: Any, key: str ='HourlyForecast_MX.gz', partition_date: Optional[str]=today) -> None:
        """Upload object into s3

        Args:
            s3client (boto3.client): Client of s3
            bucket (str): Bucket name
            obj (Any): Object
            key (str, optional): Name of the object. Defaults to 'HourlyForecast_MX.gz'.
            partition_date (datetime, optional): Prefix of the object. Defaults to datetime.datetime.today().date().isoformat().
        """
        key = f'{partition_date}/{key}'
        self.s3_client.put_object(Bucket=bucket, Body=obj, Key=key)
        self.logger.info("Upload to s3 successful")
        return None


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
    s3_client = S3_Connector(os.environ['S3_ACCESS_KEY_ID'], os.environ['S3_SECRET_ACCESS_KEY'])
    file = s3_client.download_s3_json(partition_date='2023-09-02', bucket=os.environ["S3_BUCKET"], file_name="HourlyForecast_MX.gz")
    print(type(file))
    