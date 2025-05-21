from src.processors.ext_configs import IExtConfig
from .base import BaseExt
from .sql_server import SqlServerExt
from .aws_storage import AwsStorageExt


def get_ext_class(config: IExtConfig):
    ext_type = config["type"]
    if ext_type == "awsStorage":
        return AwsStorageExt
    elif ext_type == "sqlServer":
        return SqlServerExt
    else:
        raise ValueError(f"extractor type {ext_type} not exists")
