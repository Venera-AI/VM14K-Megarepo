from typing import Generic, TypeVar
from src.custom_types.common import BaseDict
from src.processors.query_builders.click_house import BaseQueryBuilder

T = TypeVar("T", bound=BaseDict)


class ClickHouseRepo(Generic[T]):
    query_builder: BaseQueryBuilder
    pass
