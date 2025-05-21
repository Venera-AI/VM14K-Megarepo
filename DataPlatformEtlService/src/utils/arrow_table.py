import duckdb
import pyarrow as pa


class ArrowTableUtil:
    @staticmethod
    def transform_table_with_duckdb(arrow_table: pa.Table, sql_query: str, table_name: str) -> pa.Table:
        rel_from_arrow = duckdb.arrow(arrow_table)
        rel_from_arrow.query(table_name, sql_query)
        return rel_from_arrow.arrow()
