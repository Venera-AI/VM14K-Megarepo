from typing import Literal, NotRequired
from src.dtos import GenerateTypeAdapter, BaseDTO
from src.processors.conn_configs import IAwsStorageConnConfig
from src.processors.ext_configs import IBaseExtConfig


class __IConfig__(BaseDTO):
    connConfig: IAwsStorageConnConfig
    bucketName: str
    prefix: NotRequired[str]
    pathGlobFilter: NotRequired[str]
    recursive: bool
    removePrefixPath: NotRequired[str]
    fileType: Literal["json", "parquet"]
    compressType: Literal["bzip2"] | None


class IAwsStorageExtConfig(IBaseExtConfig):
    type: Literal["awsStorage"]
    config: __IConfig__


AwsStorageExtConfigDTO = GenerateTypeAdapter[IAwsStorageExtConfig](IAwsStorageExtConfig)
