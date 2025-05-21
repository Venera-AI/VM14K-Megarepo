from typing import Literal
from typing_extensions import NotRequired
from src.dtos import GenerateTypeAdapter, BaseDTO
from src.processors.conn_configs import ISqlServerConnConfig
from src.processors.ext_configs import IBaseExtConfig


class __IConfig__(BaseDTO):
    connConfig: ISqlServerConnConfig
    schema: str
    table: str
    filterSql: NotRequired[str]
    batchSize: int
    incrementalField: str
    orderSql: str


class ISqlServerExtConfig(IBaseExtConfig):
    type: Literal["sqlServer"]
    config: __IConfig__


SqlServerExtConfigDTO = GenerateTypeAdapter[ISqlServerExtConfig](ISqlServerExtConfig)
