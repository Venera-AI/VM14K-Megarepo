from typing import Dict, List
from sqlalchemy.exc import IntegrityError
from src.common.responses import CErrorResponse
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.entities import FExtConnDep
from src.repositories import FExtConnDepRepo
from src.services import PostgresService
from src.utils.postgres import PostgresErrorUtils


class FExtConnDepService:

    def __init__(self, session_scope):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)

    async def get_conn_deps(self, f_ext_config_name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FExtConnDepRepo,
            conditions={
                "logical": "and",
                "conditions": [
                    {
                        "field": "fExtgConfigNameValue",
                        "operator": "=",
                        "value": f_ext_config_name_value,
                    },
                ],
            },
        )
        return records

    async def add(self, new_records: List[FExtConnDep], returning: bool = False):
        try:
            with self.session_scope():
                await self.postgres_service.insert_many(repo=FExtConnDepRepo, records=new_records, returning=returning)
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(http_code=400, status_code=400, message="pair ext and value already exists")


F_EXT_CONN_DEP_SERVICE = FExtConnDepService(session_scope=ETL_METADATA_SESSION_SCOPE)
