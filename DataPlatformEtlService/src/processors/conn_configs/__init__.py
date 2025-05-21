from typing import Annotated

from pydantic import Field
from src.dtos import GenerateTypeAdapter
from src.dtos.base import PYDANTIC_DISCRIMINATOR_KEY
from .base import IBaseConnConfig
from .spark import ISparkConnConfig, SparkConnConfigDTO
from .gcp import IGcpStorageConnConfig, GcpStorageConnConfigDTO
from .aws import IAwsStorageConnConfig, AwsStorageConnConfigDTO
from .hugging_face import IHuggingFaceConnConfig, HuggingFaceConnConfigDTO
from .sql_server import ISqlServerConnConfig, SqlServerConnConfigDTO


IConnConfig = Annotated[
    ISparkConnConfig | IAwsStorageConnConfig | IGcpStorageConnConfig | IHuggingFaceConnConfig | ISqlServerConnConfig,
    Field(discriminator="type"),
]

ConnConfigDTO = GenerateTypeAdapter[IConnConfig](IConnConfig)
