from typing import Annotated

from pydantic import Field

from src.dtos import GenerateTypeAdapter
from .base import IBaseExtConfig
from .sql_server import ISqlServerExtConfig, SqlServerExtConfigDTO
from .aws_storage import IAwsStorageExtConfig, AwsStorageExtConfigDTO

IExtConfig = Annotated[
    ISqlServerExtConfig | IAwsStorageExtConfig,
    Field(discriminator="type"),
]

ExtConfigDTO = GenerateTypeAdapter[IExtConfig](IExtConfig)
