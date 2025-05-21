import os


class EtlMetadataConsts:
    CREDENTIALS = {"dns": os.environ["ETL_BACKEND_DNS"], "min_conn": 8, "max_conn": 4}
    SCHEMA = "etls"
    SPARK_DNS = os.environ["SPARK_DNS"]
