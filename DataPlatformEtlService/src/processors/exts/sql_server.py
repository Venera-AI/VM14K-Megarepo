from typing import Dict, List

from sqlalchemy.orm import Session
from src.db.sessions import BaseSession
from src.processors.common import IProcessDataBlock
from src.processors.conns.sql_server import SqlServerConnPool
from src.processors.ext_configs import SqlServerExtConfigDTO, ISqlServerExtConfig
from src.processors.exts import BaseExt
from src.processors.query_builders.postgres import (
    SqlConditionInterface,
    BaseQueryBuilder,
    SqlQuery,
    literal_object,
)
from src.processors.repositories.postgres import PostgresRepo
from src.utils.logger import LOGGER


class SqlServerExt(BaseExt[ISqlServerExtConfig]):
    def __init__(
        self,
        config,
        execution_time,
    ):
        super().__init__(
            config=config,
            execution_time=execution_time,
        )
        self.config = self.root_config["config"]
        self.page = 0
        self.batch_size = self.config["batchSize"]
        self.connector = BaseSession[Session](
            inputs=self.config["connConfig"], pool_class=SqlServerConnPool
        ).generate_session_scope_func()
        self.query_builder = BaseQueryBuilder(table=self.config["table"], schema=self.config["schema"])
        self.incremental_type = None
        if self.private_incremental_field in self.selected_fields:
            raise ValueError("selected_fields contains private incremental field")
        self.selected_field_sql = [f"{literal_object(field)}" for field in selected_fields]
        self.selected_field_sql += (
            f"cast({literal_object(self.config['incrementalField'])} as text)"
            f" as {literal_object(self.private_incremental_field)}"
        )
        self.selected_field_sql = ", ".join(self.selected_field_sql)
        # self.selected_fields = list(set(self.selected_fields))

    @classmethod
    def is_compatible_with_transfer_copy_file(cls):
        return False

    @classmethod
    def validate_root_config(cls, config):
        return SqlServerExtConfigDTO.validate_python(config)

    async def get_incremental_type(self, init_metadata_values: Dict):
        if self.incremental_type is None:
            sql = f"""
                select top 1
                cast(SQL_VARIANT_PROPERTY({literal_object(self.config['incrementalField'])},'BaseType') as VARCHAR(255))
                as type
                from {self.query_builder.full_table_name})
            """
            with self.connector() as session:
                records = PostgresRepo.row_factory(session.connection().exec_driver_sql(sql))
            self.incremental_type = records[0]["type"]
        return self.incremental_type

    def generate_query_filter(self):
        if self.init_incremental_value is not None:
            incremental_mode = ">" if self.root_config["incrementalMode"] != ">=" else ">="
            conditions: SqlConditionInterface = {
                "logical": "and",
                "conditions": [
                    {
                        "field": self.config["incrementalField"],
                        "operator": incremental_mode,
                        "value": SqlQuery(f"convert({type}, %s)", [self.init_incremental_value]),
                    },
                ],
            }
            query = self.query_builder.where(conditions=conditions)
            if self.config.get("filterSql", None) is not None:
                query.sql = f"{query.sql} AND {self.config['filterSql']}"
            return query
        else:
            return SqlQuery(sql=self.config["filterSql"])

    async def extract(self, init_metadata_values: Dict) -> List[IProcessDataBlock]:
        filter_query = self.generate_query_filter()
        with self.connector() as session:
            # Exec query
            sql = f"""
                SELECT {self.selected_field_sql}
                FROM (
                    SELECT *
                    FROM {self.query_builder.full_table_name}
                ) _
                {filter_query.add_where_operator()}
                ORDER BY {self.config["orderSql"]}
                OFFSET %s
                LIMIT %s
            """
            params = filter_query.params + [self.page * self.batch_size, self.batch_size]
            LOGGER.info("[ETL][EXTRACTOR] %s" % sql)
            LOGGER.info("[ETL][EXTRACTOR] %s" % params)
            records = PostgresRepo.row_factory(cur=session.connection().exec_driver_sql(sql, tuple(params)).cursor)
            incremental_values = []
            for record in records:
                incremental_values.append(record[self.private_incremental_field])
                del record[self.private_incremental_field]
            LOGGER.info(f"[ETL][EXTRACTOR] extracted {len(records)} rows")
            self.page += 1
            # Process records into data block object and return (or send it to Queue in future implementation)
            if len(records) == 0:
                # Turnoff extractor if has no more data
                self.process_shared_memory["processState"]["tapIsDone"] = True
                return [
                    {
                        "isEnd": True,
                        "data": [],
                        "incrementalValues": [],
                    }
                ]
            data_block: IProcessDataBlock = {
                "isEnd": False,
                "data": records,
                "incrementalValues": incremental_values,
            }
        return [data_block]
