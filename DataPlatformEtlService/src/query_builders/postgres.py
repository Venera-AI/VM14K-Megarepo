from typing import Dict, Generic, Literal, Tuple, Union, List, Optional, TypedDict, Any
from typing_extensions import NotRequired, TypeVar
import numpy as np
import pandas as pd

from src.custom_types.common import BaseDict

OPERATORS = Literal["IS", "IS NOT", "IN", "NOT IN", "LIKE", "NOT LIKE", "=", "!=", ">", "<", ">=", "<="]


def literal_object(text, alias=""):
    alias_string = "" if alias == "" else f'"{alias}".'
    return f'{alias_string}"{text}"'


def literal_objects(list_text: List[str], alias=""):
    return [literal_object(text=text, alias=alias) for text in list_text]


class SqlText:
    def __init__(self, text: str):
        self.text = text


class ConditionItem(TypedDict, total=False):
    field: str | SqlText
    operator: OPERATORS
    value: Any


# SqlConditionInterface = TypedDict(
#     "SqlConditionInterface",
#     {
#         "logical": Literal["and", "or"],
#         "conditions": List[Union[__ConditionItem__, "SqlConditionInterface"]],
#         "alias": NotRequired[str],
#     },
# )

ConditionTypeVar = TypeVar(
    "ConditionTypeVar",
    default=Union[ConditionItem, "SqlConditionInterface"],
    bound=Union[ConditionItem, "SqlConditionInterface"],
    covariant=True,
)


class SqlConditionInterface(Generic[ConditionTypeVar], TypedDict):
    logical: Literal["and", "or"]
    conditions: List[ConditionTypeVar]
    alias: NotRequired[str]


class SqlOrderByByInterface(TypedDict):
    field: str | SqlText
    direction: Literal["ASC", "DESC"]
    alias: NotRequired[str]


class SqlPaginationInterface(TypedDict):
    conditions: SqlConditionInterface
    orderBys: List[SqlOrderByByInterface]
    page: int
    pageSize: int


class SqlQuery:
    def __init__(self, sql, params=[], columns=[]):
        self.sql: str = sql
        self.params: List = params
        self.columns: List[str] = columns

    def copy(self):
        new_object = SqlQuery(sql=self.sql, params=self.params.copy(), columns=self.columns.copy())
        return new_object

    def add_where_operator(self):
        if self.sql is not None and self.sql.strip() != "":
            return f"WHERE {self.sql}"
        return self.sql

    def add_order_by_operator(self):
        if self.sql is not None and self.sql.strip() != "":
            return f"ORDER BY {self.sql}"
        return self.sql


SqlColumnInterface = str | SqlText | List[str] | List[SqlText] | Tuple[str] | Tuple[SqlText]


class BaseQueryBuilder:
    def __init__(self, table: str, schema: str | None):
        self.schema = schema
        self.table = table
        if self.schema is not None:
            self.table_type = f"{literal_object(self.schema)}.{literal_object(self.table + 'Type')}"
            self.full_table_name = f"{literal_object(self.schema)}.{literal_object(self.table)}"
        else:
            self.table_type = f"{literal_object(self.table + 'Type')}"
            self.full_table_name = f"{literal_object(self.table)}"

    @classmethod
    def set_values(cls, left_sequences: List[str], right_sequences: List[str]):
        results = []
        for i in range(len(left_sequences)):
            left_seq = left_sequences[i]
            right_seq = right_sequences[i]
            results.append(f"{left_seq} = {right_seq}")
        return results

    @classmethod
    def values(
        cls,
        records: List[BaseDict] | pd.DataFrame | Any,
        text_clauses: Optional[Dict[str, SqlText]] = {},
        execute_batch=False,
    ):
        if len(records) == 0:
            return SqlQuery(sql="")
        data = pd.DataFrame(records).replace({np.nan: None})
        row = data.shape[0]
        col = data.shape[1]
        if execute_batch:
            params = data.values.tolist()
        else:
            params = data.values.flatten().tolist()
        columns = list(data.columns)
        sql_value = ["%s"] * col
        if text_clauses:
            text_columns = list(text_clauses.keys())
            text_values = [text_clauses[i].text for i in text_columns]
            columns += text_columns
            sql_value += text_values
        sql_value = ", ".join(sql_value)
        sql_value = f"({sql_value})"
        sql_values = ", ".join([sql_value] * row)
        sql_values = f"{sql_values}"
        return SqlQuery(sql=sql_values, params=params, columns=columns)

    @classmethod
    def where(cls, conditions: SqlConditionInterface, alias=None):
        """
        alias: Optional
            can set alias in any condition, alias parameter will pass and use in sub condition
            alias in conditions parameter will overwrite alias parameter
        """
        sql = []
        alias_string = ""
        params = []
        sub_queries: List[SqlQuery] = []
        if "alias" in conditions:
            alias = conditions["alias"]
        if alias is not None and alias != "":
            alias_string = f"{literal_object(alias)}."
        logical = conditions["logical"]
        for sub_condition in conditions["conditions"]:
            if "conditions" in sub_condition:
                sub_query = cls.where(conditions=sub_condition, alias=alias)
            else:
                sub_sql = []
                sub_params = []
                field = sub_condition["field"]
                value = sub_condition["value"]
                operator = sub_condition["operator"]
                # if operator not in OPERATORS:
                #     raise ValueError("operator invalid")
                if isinstance(value, (list, tuple)):
                    value_sql = ", ".join(["%s" for _ in value])
                    value_sql = f"({value_sql})"
                    value_params = value
                    sub_params += value_params
                    if operator == "=":
                        operator = "IN"
                    elif operator == "!=":
                        operator = "NOT IN"
                elif isinstance(value, SqlText):
                    value_sql = value.text
                else:
                    if operator == "IN":
                        operator = "="
                    elif operator == "NOT IN":
                        operator = "!="
                    value_sql = "%s"
                    value_params = [value]
                    if value is None:
                        if operator == "=":
                            operator = "IS"
                        elif operator == "!=":
                            operator = "IS NOT"
                    sub_params += value_params
                if isinstance(field, SqlText):
                    sub_sql = f"{field.text} {operator} {value_sql}"
                else:
                    sub_sql = f"{alias_string}{literal_object(field)} {operator} {value_sql}"
                sub_query = SqlQuery(sql=sub_sql, params=sub_params, columns=None)
            sub_queries.append(sub_query)
        if len(sub_queries) != 0:
            sql = []
            params = []
            for sub_query in sub_queries:
                sql.append(sub_query.sql)
                params += sub_query.params
            sql = f" {logical.upper()} ".join(sql)
            return SqlQuery(sql=f"({sql})", params=params, columns=None)
        else:
            raise ValueError("[REPO]: conditions list is empty")

    @classmethod
    def order_by(cls, order_bys: List[SqlOrderByByInterface]):
        sql = []
        params = []
        list_fields = [sort_by["field"] for sort_by in order_bys]
        list_directions = [sort_by["direction"] for sort_by in order_bys]
        list_alias = [sort_by.get("alias", None) for sort_by in order_bys]
        for i in range(len(list_fields)):
            field = list_fields[i]
            direction = list_directions[i]
            alias = list_alias[i]
            alias = f"{literal_object(alias)}." if alias is not None else ""
            if isinstance(field, SqlText):
                sql.append(f"{field.text} {direction}")
            else:
                sql.append(f"{alias}{literal_object(field)} {direction}")
        sql = ", ".join(sql)
        return SqlQuery(sql=sql, params=params, columns=None)

    @classmethod
    def limit_offset(cls, limit: int | None = None, offset: int | None = None):
        sql = []
        params = []
        if limit is not None:
            sql.append("LIMIT %s")
            params.append(limit)
        if offset is not None:
            sql.append("OFFSET %s")
            params.append(offset)
        return SqlQuery(sql=" ".join(sql), params=params, columns=None)
