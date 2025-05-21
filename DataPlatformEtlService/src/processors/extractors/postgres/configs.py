from typing import Annotated, List

from annotated_types import Len
from src.modules.base.dto import BaseDTO
from src.modules.processors.extractors import OrderFieldInterface


class PostgresExtConfig(BaseDTO):
    schemaName: str
    tableName: str
    batchSize: int
    orderFields: Annotated[List[OrderFieldInterface], Len(min_length=1)]
