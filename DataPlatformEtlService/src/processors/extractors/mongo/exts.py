from typing import Any, List, cast
from src.modules.processors.connectors.mongo import MongoSession
from src.modules.processors.extractors.base import BaseExt
from src.modules.processors.extractors.mongo.configs import MongoExtConfig
from src.modules.processors.extractors.mongo.interfaces import MongoDBI
from src.modules.processors.query_builders.base import ConditionInterface
from src.modules.processors.query_builders.mongo import MongoQueryBuilder
from src.utils.logger import LOGGER


class MongoExt(BaseExt[MongoExtConfig, MongoSession]):
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
            self.selected_fields += [self.config["incrementalField"]] + [
                field["field"] for field in self.config["orderFields"]
            ]
            self.selected_fields = list(set(self.selected_fields))
        self.page = 0
        self.batch_size = self.config["batchSize"]

    async def extract(self):
        query = []
        if len(self.selected_fields) != 0:
            query.append({"$project": {field: 1 for field in self.selected_fields}})
        incremental_field = self.config["incrementalField"]
        order_fields = self.config["orderFields"]
        order_query = await MongoQueryBuilder.sort(order_fields=order_fields)
        query.append({"$sort": order_query})
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
        filters = self.config.get("filters", None)
        if filters is not None:
            conditions.append(cast(ConditionInterface, filters))
        if len(conditions) != 0:
            full_filters: ConditionInterface = {
                "logical": "and",
                "conditions": cast(Any, conditions),
            }
            filter_query = await MongoQueryBuilder.where(conditions=full_filters)
            query.append({"$match": filter_query})
        query.append({"$skip": self.page * self.batch_size})
        query.append({"$limit": self.page * self.batch_size + self.batch_size})
        with self.connector() as session:
            collection = session.client.get_database(self.config["databaseName"]).get_collection(
                self.config["collectionName"]
            )
            LOGGER.info(query)
            cursor = collection.aggregate(query)
            records = list(cursor)
            self.page += 1
            data_blocks: List[MongoDBI] = []
            if len(records) == 0:
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
            data_blocks.append(
                {
                    "length": len(records),
                    "incrementalValues": [record[incremental_field] for record in records],
                    "isEnd": False,
                    "data": records,
                }
            )
        return data_blocks
