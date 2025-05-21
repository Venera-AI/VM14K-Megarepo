import numpy as np
import pandas as pd
import psycopg2.extras
from contextlib import _GeneratorContextManager
from typing import Callable, Dict, List, Literal, Tuple, Type, TypeVar, Union, overload

from sqlalchemy.orm import Session
from src.common.consts import MessageConsts
from src.common.responses import CErrorResponse
from src.custom_types.common import BaseDict
from src.entities import BaseEntity
from src.processors.query_builders.sql_server import (
    BaseQueryBuilder,
    ConditionItem,
    SqlConditionInterface,
    SqlText,
    literal_object,
    literal_objects,
)
from src.processors.repositories import SqlServerRepo
from src.utils.logger import LOGGER


T = TypeVar("T", bound=BaseEntity)


class SqlServerService:

    def __init__(self, session_scope: Callable[..., _GeneratorContextManager[Session]]):
        self.session_scope = session_scope

    async def execute_raw_query(self, sql: str, params: Union[Tuple, List] = ()):
        with self.session_scope() as session:
            LOGGER.debug(f"[SQL]: {sql}")
            return session.connection().exec_driver_sql(sql, tuple(params)).cursor

    async def executemany_raw_query(self, sql: str, params: Union[Tuple, List] = ()):
        with self.session_scope() as session:
            LOGGER.debug(f"[SQL]: {sql}")
            cursor = session.connection().connection.cursor()
            psycopg2.extras.execute_batch(cursor, sql, tuple(params))
            return cursor

    async def get_all(self, repo: Type[SqlServerRepo[T]]) -> List[T]:
        query = repo.get_all()
        return repo.row_factory(await self.execute_raw_query(sql=query.sql, params=query.params))

    async def get_by_id(self, repo: Type[SqlServerRepo[T]], _id) -> T | None:
        query = repo.get_by_id(_id)
        records = repo.row_factory(await self.execute_raw_query(sql=query.sql, params=query.params))
        if len(records) == 0:
            return None
        return records[0]

    async def get_exist_by_id(self, repo: Type[SqlServerRepo[T]], _id) -> T:
        record = await self.get_by_id(repo, _id)
        if record is None:
            raise CErrorResponse(http_code=404, status_code=404, message=MessageConsts.NOT_FOUND)
        return record

    async def get_by_condition(self, repo: Type[SqlServerRepo[T]], conditions: SqlConditionInterface) -> List[T]:
        query = repo.get_by_condition(conditions=conditions)
        return repo.row_factory(await self.execute_raw_query(query.sql, query.params))

    @overload
    async def insert(self, repo: Type[SqlServerRepo[T]], record: T | BaseDict, returning: Literal[True]) -> T: ...

    @overload
    async def insert(self, repo: Type[SqlServerRepo[T]], record: T | BaseDict, returning: Literal[False]) -> None: ...

    @overload
    async def insert(self, repo: Type[SqlServerRepo[T]], record: T | BaseDict) -> None: ...

    async def insert(self, repo: Type[SqlServerRepo[T]], record: T | BaseDict, returning: bool = False) -> T | None:
        query = repo.insert(record=record, returning=returning)
        cursor = await self.execute_raw_query(sql=query.sql, params=query.params)
        if returning:
            return repo.row_factory(cursor)[0]
        return None

    @overload
    async def insert_many(
        self, repo: Type[SqlServerRepo[T]], records: List[T | BaseDict], returning: Literal[True]
    ) -> List[T]: ...

    @overload
    async def insert_many(
        self, repo: Type[SqlServerRepo[T]], records: List[T | BaseDict], returning: Literal[False]
    ) -> None: ...

    @overload
    async def insert_many(
        self, repo: Type[SqlServerRepo[T]], records: List[T | BaseDict], returning: bool = False
    ) -> List[T] | None: ...

    async def insert_many(
        self, repo: Type[SqlServerRepo[T]], records: List[T | BaseDict], returning: bool = False
    ) -> List[T] | None:
        query = repo.insert_many(records=records, returning=returning)
        cursor = await self.execute_raw_query(sql=query.sql, params=query.params)
        if returning:
            return repo.row_factory(cursor)
        return None

    @overload
    async def update(
        self,
        repo: Type[SqlServerRepo[T]],
        record: T | BaseDict,
        identity_columns: List[str],
        returning: Literal[True],
        text_clauses: Dict[str, SqlText] = {},
    ) -> T: ...

    @overload
    async def update(
        self,
        repo: Type[SqlServerRepo[T]],
        record: T | BaseDict,
        identity_columns: List[str],
        returning: Literal[False],
        text_clauses: Dict[str, SqlText] = {},
    ) -> None: ...

    async def update(
        self,
        repo: Type[SqlServerRepo[T]],
        record: T | BaseDict,
        identity_columns: List[str],
        returning: bool = False,
        text_clauses: Dict[str, SqlText] = {},
    ) -> T | None:
        query = repo.update(
            record=record, identity_columns=identity_columns, returning=returning, text_clauses=text_clauses
        )
        cursor = await self.execute_raw_query(sql=query.sql, params=query.params)
        if returning:
            return repo.row_factory(cursor)[0]
        return None

    @overload
    async def update_many(
        self,
        repo: Type[SqlServerRepo[T]],
        records: List[T],
        identity_columns: List[str],
        returning: Literal[True],
        text_clauses: Dict[str, SqlText] = {},
    ) -> List[T]: ...

    @overload
    async def update_many(
        self,
        repo: Type[SqlServerRepo[T]],
        records: List[T] | List[BaseDict],
        identity_columns: List[str],
        returning: Literal[False],
        text_clauses: Dict[str, SqlText] = {},
    ) -> None: ...

    async def update_many(
        self,
        repo: Type[SqlServerRepo[T]],
        records: List[T] | List[BaseDict],
        identity_columns: List[str],
        returning: bool = False,
        text_clauses: Dict[str, SqlText] = {},
    ) -> List[T] | None:
        query = repo.update_many(
            records=records, identity_columns=identity_columns, returning=returning, text_clauses=text_clauses
        )
        cursor = await self.execute_raw_query(sql=query.sql, params=query.params)
        if returning:
            updated_records = repo.row_factory(cursor)
            return updated_records
        return None

    async def fast_insert_into_temp(
        self,
        target_query_builder: BaseQueryBuilder,
        records: List[T] | List[BaseDict] | pd.DataFrame,
        temp_table: str,
        text_clauses: Dict[str, SqlText] = {},
    ):
        if "#" not in temp_table:
            raise Exception(f"{temp_table} temp table not contain #")
        records = pd.DataFrame(records).replace({np.nan: None})
        chunk_size = 1
        query_values = target_query_builder.values(
            records=records.iloc[:chunk_size], text_clauses=text_clauses, execute_batch=True
        )
        if len(records) == 0:
            return query_values
        sql_columns = ", ".join(literal_objects(query_values.columns))
        params = records.values.tolist()
        with self.session_scope():
            await self.execute_raw_query(
                "CREATE TEMPORARY TABLE IF NOT EXISTS %s OF %s;"
                % (literal_object(temp_table), target_query_builder.table_type)
            )
            sql = "INSERT INTO %s (%s) VALUES %s" % (
                literal_object(temp_table),
                sql_columns,
                query_values.sql,
            )
            for i in range(0, len(params), 10000):
                await self.executemany_raw_query(sql, tuple(params[i : i + 10000]))
        return query_values

    def _validate_non_nested_condition(self, identity_columns: SqlConditionInterface):
        for condition in identity_columns["conditions"]:
            if "conditions" in condition:
                raise Exception("condition is not valid")

    async def fast_upsert_from_source_table(
        self,
        target_query_builder: BaseQueryBuilder,
        source_query_builder: BaseQueryBuilder,
        join_conditions: SqlConditionInterface[ConditionItem],
        insert_conditions: SqlConditionInterface[ConditionItem],
        upsert_columns: List[str],
        is_update=True,
        is_insert=True,
    ):
        self._validate_non_nested_condition(insert_conditions)
        self._validate_non_nested_condition(join_conditions)
        source_alias = source_query_builder.table
        target_alias = target_query_builder.table
        query_join_conditions = BaseQueryBuilder.where(conditions=join_conditions)
        # sql update
        sql_set_columns = ", ".join(
            BaseQueryBuilder.set_values(
                left_sequences=literal_objects(list_text=upsert_columns),
                right_sequences=literal_objects(list_text=upsert_columns, alias=source_alias),
            )
        )
        sql_update = f"""
            UPDATE {target_query_builder.full_table_name} AS {literal_object(target_alias)}
            SET {sql_set_columns}
            FROM {source_query_builder.full_table_name} AS {literal_object(source_alias)}
            {query_join_conditions.add_where_operator()}
        """
        # sql insert
        sql_select_columns = ", ".join(literal_objects(list_text=upsert_columns, alias=source_alias))
        sql_insert_columns = ", ".join(literal_objects(list_text=upsert_columns))
        query_insert_conditions = BaseQueryBuilder.where(insert_conditions)
        sql_insert_conditions = f"""
                LEFT JOIN {target_query_builder.full_table_name} {literal_object(target_alias)}
                    ON {query_join_conditions.sql}
                {query_insert_conditions.add_where_operator()}
            """
        sql_insert = f"""
            --SET NOCOUNT ON;
            INSERT INTO {target_query_builder.full_table_name} ({sql_insert_columns})
            SELECT {sql_select_columns}
            FROM {literal_object(source_alias)}
            {sql_insert_conditions}
        """
        list_sql = [sql_update] if is_update and len(join_conditions["conditions"]) != 0 else []
        if is_insert:
            list_sql += [sql_insert]
        sql = ";\n".join(list_sql)
        with self.session_scope():
            await self.execute_raw_query(sql, params=query_join_conditions.params + query_insert_conditions.params)
        return

    async def fast_insert_on_conflict_do_nothing(
        self,
        target_query_builder: BaseQueryBuilder,
        temp_table: str,
        records: List[T] | List[BaseDict] | pd.DataFrame,
        join_conditions: SqlConditionInterface[ConditionItem],
        insert_conditions: SqlConditionInterface[ConditionItem],
        text_clauses: Dict[str, SqlText] = {},
    ):
        self._validate_non_nested_condition(join_conditions)
        self._validate_non_nested_condition(insert_conditions)
        with self.session_scope():
            # temp_table = f"#{cls.query_builder.table}"
            if len(records) > 0:
                query = await self.fast_insert_into_temp(
                    target_query_builder=target_query_builder,
                    records=records,
                    temp_table=temp_table,
                    text_clauses=text_clauses,
                )
                await self.fast_upsert_from_source_table(
                    target_query_builder=target_query_builder,
                    source_query_builder=BaseQueryBuilder(table=temp_table, schema=None),
                    join_conditions=join_conditions,
                    insert_conditions=insert_conditions,
                    upsert_columns=query.columns,
                    is_update=False,
                )
                await self.execute_raw_query(f'DROP TABLE "{temp_table}"')
        return

    async def fast_upsert(
        self,
        target_query_builder: BaseQueryBuilder,
        temp_table: str,
        records: List[T] | List[BaseDict] | pd.DataFrame,
        join_conditions: SqlConditionInterface[ConditionItem],
        insert_conditions: SqlConditionInterface[ConditionItem],
        text_clauses: Dict[str, SqlText] = {},
        is_update=True,
        is_insert=True,
    ):
        with self.session_scope():
            # temp_table = f"#{cls.query_builder.table}"
            if len(records) > 0:
                query = await self.fast_insert_into_temp(
                    target_query_builder=target_query_builder,
                    records=records,
                    temp_table=temp_table,
                    text_clauses=text_clauses,
                )
                await self.fast_upsert_from_source_table(
                    target_query_builder=target_query_builder,
                    source_query_builder=BaseQueryBuilder(table=temp_table, schema=None),
                    join_conditions=join_conditions,
                    insert_conditions=insert_conditions,
                    upsert_columns=query.columns,
                    is_insert=is_insert,
                    is_update=is_update,
                )
                await self.execute_raw_query(f'DROP TABLE "{temp_table}"')
        return

    async def get_order_columns(self, repo: Type[SqlServerRepo[T]], exclude_columns: List[str] = []) -> List[str]:
        conditions: SqlConditionInterface = {
            "logical": "and",
            "conditions": [
                {
                    "field": "table_schema",
                    "operator": "=",
                    "value": repo.query_builder.schema,
                },
                {
                    "field": "table_name",
                    "operator": "=",
                    "value": repo.query_builder.table,
                },
            ],
        }
        if exclude_columns is not None and len(exclude_columns) > 0:
            conditions: SqlConditionInterface = {
                "logical": "and",
                "conditions": [
                    {
                        "field": "column_name",
                        "operator": "NOT IN",
                        "value": exclude_columns,
                    },
                    conditions,
                ],
            }
        condition_query = repo.query_builder.where(conditions)
        sql = f"""
            SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            {condition_query.add_where_operator()}
            ORDER BY column_name ASC
        """
        cur = await self.execute_raw_query(sql=sql, params=condition_query.params)
        return [row["column_name"] for row in repo.row_factory(cur=cur)]
