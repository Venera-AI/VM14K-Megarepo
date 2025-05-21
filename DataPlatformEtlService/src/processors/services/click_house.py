import pandas as pd
from contextlib import _GeneratorContextManager
from typing import Callable, Type, TypeVar

from clickhouse_connect.driver.client import Client

from src.entities import BaseEntity
from src.processors.repositories import ClickHouseRepo


T = TypeVar("T", bound=BaseEntity)


class ClickHouseService:

    def __init__(self, session_scope: Callable[..., _GeneratorContextManager[Client]]):
        self.session_scope = session_scope

    async def insert_many(self, repo: Type[ClickHouseRepo], records: list[T]):
        with self.session_scope() as client:
            df = pd.DataFrame(records)
            ic = client.create_insert_context(
                table=repo.query_builder.full_table_name, column_names=df.columns.tolist(), data=df.values.tolist()
            )
            client.insert(context=ic)
