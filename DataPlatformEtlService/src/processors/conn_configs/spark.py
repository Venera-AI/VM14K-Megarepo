from typing import Literal

from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    dns: str


class ISparkConnConfig(IBaseConnConfig):
    type: Literal["spark"]
    config: __IConfig__


SparkConnConfigDTO = GenerateTypeAdapter[ISparkConnConfig](ISparkConnConfig)
