from typing import Literal

from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    dns: str


class IClickHouseConnConfig(IBaseConnConfig):
    type: Literal["clickHouse"]
    config: __IConfig__


ClickHouseConnConfigDTO = GenerateTypeAdapter[IClickHouseConnConfig](IClickHouseConnConfig)
