from contextlib import _GeneratorContextManager
from typing import Any, Dict, List, TypeVar, Generic, Union, Callable
from bson import ObjectId
from src.modules.processors.connectors.mongo import MongoSession
from src.modules.processors.query_builders.mongo import MongoQueryBuilder
from src.modules.processors.query_builders.postgres import ConditionInterface

T = TypeVar("T", bound=Union[Dict, Any])


class MongoRepo(Generic[T]):
    session_scope: Callable[..., _GeneratorContextManager[MongoSession]]
    query_builder: MongoQueryBuilder
    collection_name: str
    __pk_field = "_id"

    @classmethod
    async def insert(cls, record):
        """
        create query_callback, actual execute on end of transaction
        """
        with cls.session_scope() as session:
            record[cls.__pk_field] = ObjectId()
            session.query_callbacks.append(
                lambda: session.db.get_collection(cls.collection_name).insert_one(record.copy())
            )
        return record

    @classmethod
    async def insert_many(cls, records):
        """
        create query_callback, actual execute on end of transaction
        """
        with cls.session_scope() as session:
            for record in records:
                record[cls.__pk_field] = ObjectId()
            session.query_callbacks.append(lambda: session.db.get_collection(cls.collection_name).insert_many(records))
        return records

    @classmethod
    async def get_by_condition(cls, conditions: ConditionInterface) -> List[T]:
        condition_query = await cls.query_builder.where(conditions)
        with cls.session_scope() as session:
            cursor = session.db.get_collection(cls.collection_name).find(condition_query)
            return list(cursor)
