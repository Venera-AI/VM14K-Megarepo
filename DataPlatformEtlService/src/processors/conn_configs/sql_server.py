from typing import Literal

from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    dns: str


class ISqlServerConnConfig(IBaseConnConfig):
    type: Literal["sqlServer"]
    config: __IConfig__


SqlServerConnConfigDTO = GenerateTypeAdapter[ISqlServerConnConfig](ISqlServerConnConfig)
