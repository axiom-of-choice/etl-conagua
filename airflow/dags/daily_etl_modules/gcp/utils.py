from google.cloud import bigquery
import inspect
import pandas as pd

attr = inspect.getmembers(bigquery.enums.SqlTypeNames, lambda a:not(inspect.isroutine(a)))
attrs = [a for a in attr if not(a[0].startswith('__') and a[0].endswith('__') or a[0] in ('name', 'value'))]


BQ_PD_DATA_MAPPER = {
    "STRING": "object",
    "BYTES": "object",
    "INTEGER": "int64",
    "INT64": "int64",
    "FLOAT": "float64",
    "FLOAT64": "float64",
    "NUMERIC": "float64",
    "BIGNUMERIC": "float64",
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "GEOGRAPHY": "object",
    "RECORD": "object",
    "STRUCT": "object",
    "TIMESTAMP": "datetime64[ns, UTC]",
    "DATE": "datetime64[ns]",
    "TIME": "datetime64[ns]",
    "DATETIME": "datetime64[ns]",
    "INTERVAL": "object",
}

if __name__ == "__main__":
    print(attrs)