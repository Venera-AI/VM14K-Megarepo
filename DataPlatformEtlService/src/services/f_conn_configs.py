from typing import cast
from sqlalchemy.exc import IntegrityError

from src.common.responses import CErrorResponse
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.dtos import IAddFConnConfig
from src.entities import FConnConfig
from src.processors.conn_configs import ConnConfigDTO, IConnConfig
from src.repositories import FConnConfigRepo
from src.services import PostgresService
from src.utils.data import DataUtil
from src.utils.postgres import PostgresErrorUtils


class FConnConfigService:
    JSON_COLUMNS = ["config"]

    def __init__(self, session_scope):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)

    async def get_by_name_value(self, name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FConnConfigRepo,
            conditions={
                "logical": "and",
                "conditions": [
                    {
                        "field": "nameValue",
                        "operator": "=",
                        "value": name_value,
                    },
                ],
            },
        )
        if len(records) == 0:
            return None
        return records[0]

    def generate_conn_config(self, record: FConnConfig):
        return ConnConfigDTO.validate_python(
            {
                "type": record["type"],
                "config": record["config"],
            }
        )

    async def add(self, payload: IAddFConnConfig, returning: bool = False):
        # validate config
        new_record = cast(FConnConfig, payload)
        self.generate_conn_config(new_record)
        new_record = DataUtil.dumps([new_record], self.__class__.JSON_COLUMNS)[0]
        try:
            return await self.postgres_service.insert(repo=FConnConfigRepo, record=new_record, returning=returning)
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(http_code=400, status_code=400, message="connector name value already exists")


F_CONN_CONFIG_SERVICE = FConnConfigService(session_scope=ETL_METADATA_SESSION_SCOPE)
