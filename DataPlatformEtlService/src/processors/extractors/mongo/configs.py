from annotated_types import Len
from typing import Annotated, Literal, NotRequired, List

from pydantic import Field
from src.modules.base.dto import BaseDTO
from src.modules.processors.extractors.base import FilterInterface, OrderFieldInterface


class MongoExtConfig(BaseDTO):
    databaseName: str
    collectionName: str
    filters: NotRequired[FilterInterface]
    batchSize: int
    incrementalField: str
    incrementalOperator: Annotated[Literal[">", ">="], Field(default=">")]
    orderFields: Annotated[List[OrderFieldInterface], Len(min_length=1)]
