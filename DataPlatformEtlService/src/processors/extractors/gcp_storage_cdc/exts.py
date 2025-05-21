from typing import Any, List, cast
from src.modules.base.query_builder import BaseQueryBuilder, ConditionInterface
from src.modules.base.repositories import BaseRepo
from src.modules.processors.connectors.gcp_storage_cdc import GcpStorageCdcClient
from src.modules.processors.extractors.base import BaseExt
from src.modules.processors.extractors.gcp_storage_cdc import GcpStorageCdcExtConfig
from src.modules.processors.extractors.gcp_storage_cdc.interfaces import GcpStorageCdcDBI
from src.utils.gcp import GCPStorageUtil
from src.utils.logger import LOGGER


class GcpStorageCdcExt(BaseExt[GcpStorageCdcExtConfig, GcpStorageCdcClient]):
    def __init__(
        self,
        config,
        connector,
        process_shared_memory,
        init_incremental_value,
        private_field_prefix,
        selected_fields,
        incremental_mode,
    ):
        super().__init__(
            config=config,
            connector=connector,
            process_shared_memory=process_shared_memory,
            init_incremental_value=init_incremental_value,
            private_field_prefix=private_field_prefix,
            selected_fields=selected_fields,
            incremental_mode=incremental_mode,
        )
        if len(self.selected_fields) == 0:
            self.selected_fields = []
        else:
            self.selected_fields += [self.config["incrementalField"]]
            self.selected_fields = list(set(self.selected_fields))
        self.page = 0
        self.batch_size = self.config["batchSize"]

    async def extract(self):
        # TODO: handle injection query if user is not own source

        # Generate query
        # Order part
        order_fields = self.config["orderFields"]
        order_sql = ", ".join(
            [f"\"{order_field['field']}\" {order_field['direction']}" for order_field in order_fields]
        )
        # Filter part
        incremental_field = self.config["incrementalField"]
        filter_sql = None
        params = []
        # Init filter by incremental field if not None
        conditions: List[ConditionInterface]
        if self.init_incremental_value is None:
            conditions = []
        else:
            incremental_mode = ">" if self.incremental_mode != ">=" else ">="
            conditions = [
                {
                    "logical": "and",
                    "conditions": [
                        {
                            "field": incremental_field,
                            "operator": incremental_mode,
                            "value": self.init_incremental_value,
                        },
                    ],
                }
            ]
        # Append more filter define by user
        filters = self.config.get("filters", None)
        if filters is not None:
            conditions.append(cast(ConditionInterface, filters))
        # Generate query by query_builder
        if len(conditions) != 0:
            full_filters: ConditionInterface = {
                "logical": "and",
                "conditions": cast(Any, conditions),
            }
            filter_query = await BaseQueryBuilder.where(conditions=full_filters)
            filter_sql = filter_query.sql
            params += filter_query.params
        # Add more params for sql
        params += [self.page * self.batch_size, self.batch_size]
        # Generate selected_fields
        if len(self.selected_fields) == 0:
            selected_field_sql = "*"
        else:
            selected_field_sql = ", ".join([f'"{field}"' for field in self.selected_fields])
        with self.connector() as conn, conn.postgres_session_scope() as session:
            # Exec query
            sql = f"""
                SELECT {selected_field_sql}
                FROM (
                    SELECT *
                    FROM "{self.config["cdcSchema"]}"."{self.config["cdcTable"]}"
                ) _
                {"" if filter_sql is None else f"WHERE {filter_sql}"}
                ORDER BY {order_sql}
                OFFSET %s
                LIMIT %s
            """
            LOGGER.info("[ETL][EXTRACTOR] %s" % sql)
            LOGGER.info("[ETL][EXTRACTOR] %s" % params)
            records = await BaseRepo.row_factory(cur=session.connection().exec_driver_sql(sql, tuple(params)).cursor)
            LOGGER.info(f"[ETL][EXTRACTOR] extracted {len(records)} rows")
            self.page += 1
            # Process records into data block object and return (or send it to Queue in future implementation)
            data_blocks: List[GcpStorageCdcDBI] = []
            if len(records) == 0:
                # Turnoff extractor if has no more data
                data_blocks.append(
                    {
                        "length": 0,
                        "incrementalValues": [],
                        "isEnd": True,
                        "data": [],
                    }
                )
                self.process_shared_memory["processState"]["tapIsAlive"] = False
                return data_blocks
            for record in records:
                name_object: str = record[self.config["cdcObjectNameField"]]
                with self.connector() as conn, conn.storage_session_scope() as client:
                    if self.config["fileInfo"]["type"] == "zip":
                        _, data = await GCPStorageUtil.pull_zip_to_json(
                            client=client,
                            bucket_name=record[self.config["cdcBucketNameField"]],
                            blob_name=name_object,
                            select_regex_or_list=self.config["fileInfo"]["selectRegexOrList"],
                        )
                    else:
                        raise ValueError("only support zip file")
                data_block: GcpStorageCdcDBI = {
                    "metadata": {
                        "cdcRecord": record,
                    },
                    "length": 1,
                    "incrementalValues": [record[incremental_field]],
                    "isEnd": False,
                    "data": [data],
                }
                data_blocks.append(data_block)
        return data_blocks
