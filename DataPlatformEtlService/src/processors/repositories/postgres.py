import json
from typing import Dict, Generic, List, TypeVar
import pandas as pd
from src.custom_types.common import BaseDict
from src.query_builders.postgres import (
    BaseQueryBuilder,
    SqlConditionInterface,
    SqlQuery,
    SqlText,
    literal_object,
    literal_objects,
)

T = TypeVar("T", bound=BaseDict)


class PostgresRepo(Generic[T]):
    query_builder: BaseQueryBuilder
    json_columns: List[str] = []

    @classmethod
    def dump_records(cls, records: List[T]) -> List[T]:
        results = []
        for record in records:
            result = record.copy()
            for field in cls.json_columns:
                if field in result:
                    result[field] = json.dumps(result[field])
            results.append(result)
        return results

    @classmethod
    def data_frame_factory(cls, cur) -> pd.DataFrame:
        if cur.description is None:
            return pd.DataFrame()
        columns = [column[0] for column in cur.description]
        results = [list(row) for row in cur.fetchall()]
        return pd.DataFrame(results, columns=pd.Series(columns))

    @classmethod
    def row_factory(cls, cur) -> List[T]:
        if cur.description is None:
            return []
        columns = [column[0] for column in cur.description]
        results = []
        for row in cur.fetchall():
            results.append(dict(zip(columns, row)))
        return results

    @classmethod
    def get_all(cls):
        sql = "SELECT * FROM %s" % cls.query_builder.full_table_name
        return SqlQuery(sql=sql)

    @classmethod
    def insert_many(
        cls,
        records: List[T | BaseDict] | pd.DataFrame,
        returning: bool,
        execute_batch=False,
        text_clauses: Dict[str, SqlText] = {},
    ):
        if len(records) == 0:
            raise ValueError("[ERROR][REPO]: Empty records")
        query_values = cls.query_builder.values(records, execute_batch=execute_batch, text_clauses=text_clauses)
        sql_columns = ", ".join(literal_objects(query_values.columns))
        sql_returning = " RETURNING *" if returning else ""
        sql = "INSERT INTO %s (%s)\nVALUES %s\n%s" % (
            cls.query_builder.full_table_name,
            sql_columns,
            query_values.sql,
            sql_returning,
        )
        return SqlQuery(sql=sql, params=query_values.params, columns=query_values.params)

    @classmethod
    def insert(cls, record: T | BaseDict, returning: bool):
        return cls.insert_many(records=[record], returning=returning)

    @classmethod
    def insert_on_conflict_do_nothing(cls, record: T | BaseDict, identity_columns: List[str], returning: bool):
        query = cls.insert_many(records=[record], returning=False)
        sql_returning = " RETURNING *" if returning else ""
        query.sql = (
            f"{query.sql} ON CONFLICT ({', '.join(literal_objects(identity_columns))}) DO NOTHING {sql_returning}"
        )
        return query

    @classmethod
    def update_many(
        cls,
        records: List[T] | pd.DataFrame | List[BaseDict],
        identity_columns: List[str],
        returning: bool,
        text_clauses: Dict[str, SqlText] = {},
        execute_batch=False,
    ):
        if len(identity_columns) == 0:
            raise Exception("missing require identity columns")
        query_values = cls.query_builder.values(records=records, text_clauses=text_clauses, execute_batch=execute_batch)
        update_columns = query_values.columns.copy()
        for col in identity_columns:
            update_columns.remove(col)
        sql_columns = ", ".join(literal_objects(list_text=query_values.columns))
        sql_set_columns = ", ".join(
            cls.query_builder.set_values(
                left_sequences=literal_objects(update_columns, alias=""),
                right_sequences=[
                    (
                        literal_object(c, alias="s")
                        if c not in cls.json_columns
                        else f"{literal_object(c, alias='s')}::jsonb"
                    )
                    for c in update_columns
                ],
            )
        )
        sql_join_conditions = " AND ".join(
            cls.query_builder.set_values(
                left_sequences=literal_objects(identity_columns, alias="t"),
                right_sequences=literal_objects(identity_columns, alias="s"),
            )
        )
        sql_returning = "RETURNING *" if returning else ""
        sql = f"""
            UPDATE {cls.query_builder.full_table_name} "t" SET {sql_set_columns}
            FROM (
                SELECT *
                FROM (
                    VALUES {query_values.sql}
                ) _ ({sql_columns})
            ) "s"
            WHERE {sql_join_conditions}
            {sql_returning}
        """
        return SqlQuery(sql=sql, params=query_values.params)

    @classmethod
    def update(
        cls, record: T | BaseDict, identity_columns: List[str], returning: bool, text_clauses: Dict[str, SqlText] = {}
    ):
        return cls.update_many(
            records=[record], identity_columns=identity_columns, returning=returning, text_clauses=text_clauses
        )

    @classmethod
    def get_by_id(cls, _id: int):
        conditions: SqlConditionInterface = {
            "logical": "and",
            "conditions": [{"field": "id", "operator": "=", "value": _id}],
        }
        return cls.get_by_condition(conditions=conditions)

    @classmethod
    def get_by_condition(cls, conditions: SqlConditionInterface):
        query_condition = cls.query_builder.where(conditions=conditions)
        sql = f"SELECT * FROM {cls.query_builder.full_table_name} {query_condition.add_where_operator()}"
        return SqlQuery(sql=sql, params=query_condition.params)

    @classmethod
    def lock_table(cls, mode):
        sql = f"""
            LOCK TABLE {cls.query_builder.full_table_name} IN {mode} MODE;
        """
        return SqlQuery(sql=sql)
