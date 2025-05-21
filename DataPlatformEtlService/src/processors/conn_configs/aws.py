from typing import Literal

from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    accessKeyId: str
    secretAccessKey: str


class IAwsStorageConnConfig(IBaseConnConfig):
    type: Literal["awsStorage"]
    config: __IConfig__


AwsStorageConnConfigDTO = GenerateTypeAdapter[IAwsStorageConnConfig](IAwsStorageConnConfig)
