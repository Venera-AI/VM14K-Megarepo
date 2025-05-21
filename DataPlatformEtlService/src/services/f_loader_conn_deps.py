from typing import Dict, List
from sqlalchemy.exc import IntegrityError
from src.common.responses import CErrorResponse
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.repositories import FLoaderConnDepRepo
from src.services import PostgresService
from src.utils.postgres import PostgresErrorUtils


class FLoaderConnDepService:

    def __init__(self, session_scope):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)

    async def get_conn_deps(self, f_loader_config_name_value: str):
        records = self.postgres_service.get_by_condition(
            repo=FLoaderConnDepRepo,
            conditions={
                "logical": "and",
                "conditions": [
                    {
                        "field": "fLoaderConfigNameValue",
                        "operator": "=",
                        "value": f_loader_config_name_value,
                    },
                ],
            },
        )
        return records

    async def add(self, new_records: List[Dict], returning: bool = False):
        try:
            with self.session_scope():
                await self.postgres_service.insert_many(
                    repo=FLoaderConnDepRepo, records=new_records, returning=returning
                )
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(http_code=400, status_code=400, message="pair loader and value already exists")


F_LOADER_CONN_DEP_SERVICE = FLoaderConnDepService(session_scope=ETL_METADATA_SESSION_SCOPE)
