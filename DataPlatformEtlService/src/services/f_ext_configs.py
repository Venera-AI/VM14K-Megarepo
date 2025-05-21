from typing import Dict, List
from sqlalchemy.exc import IntegrityError
from src.common.responses import CErrorResponse
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.dtos import IAddFExtConfig
from src.entities import FConnConfig, FExtConfig, FExtConnDep
from src.processors.ext_configs import IExtConfig
from src.processors.exts import get_ext_class
from src.query_builders.postgres import SqlQuery
from src.repositories import FExtConfigRepo
from src.repositories.f_conn_configs import FConnConfigRepo
from src.repositories.f_ext_conn_deps import FExtConnDepRepo
from src.repositories.postgres import PostgresRepo
from src.services import (
    PostgresService,
    F_CONN_CONFIG_SERVICE,
    FConnConfigService,
    FExtConnDepService,
    F_EXT_CONN_DEP_SERVICE,
)
from src.utils.data import DataUtil
from src.utils.postgres import PostgresErrorUtils


class FExtConfigService:
    JSON_COLUMNS = ["config"]

    def __init__(
        self, session_scope, f_conn_config_service: FConnConfigService, f_ext_conn_dep_service: FExtConnDepService
    ):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)
        self.f_conn_config_service = f_conn_config_service
        self.f_ext_conn_dep_service = f_ext_conn_dep_service

    async def get_by_name_value(self, name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FExtConfigRepo,
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

    def generate_ext_config(self, f_ext_config: FExtConfig, conn_deps: Dict[str, FConnConfig]):
        ext_config: IExtConfig = {"type": f_ext_config["type"], "config": f_ext_config["config"].copy()}
        for conn_arg_name in conn_deps:
            conn_config = self.f_conn_config_service.generate_conn_config(conn_deps[conn_arg_name])
            ext_config["config"][conn_arg_name] = conn_config
        ext_class = get_ext_class(ext_config)
        return ext_class.validate_root_config(ext_config)

    async def get_ext_config(self, name_value: str):
        sql = f"""
            SELECT fec.*, JSON_AGG(JSON_BUILD_OBJECT('connArgName', fecd."connArgName",'config',fcc.*)) AS "connDeps"
            FROM {FExtConfigRepo.query_builder.full_table_name} fec
            LEFT JOIN {FExtConnDepRepo.query_builder.full_table_name} fecd ON
                fec."nameValue"=fecd."fExtgConfigNameValue"
            LEFT JOIN {FConnConfigRepo.query_builder.full_table_name} fcc ON
                fcc."nameValue"=fecd."fConnConfigNameValue"
            WHERE fec."nameValue" = %s
            GROUP BY fec."id"
        """
        query = SqlQuery(sql=sql, params=[name_value])
        records = PostgresRepo.row_factory(await self.postgres_service.execute_raw_query(query.sql, query.params))
        if len(records) == 0:
            return None
        record = records[0]
        conn_deps: Dict[str, FConnConfig] = {
            conn_dep["connArgName"]: conn_dep["config"] for conn_dep in record["connDeps"]
        }
        f_ext_config = record.copy()
        del f_ext_config["connDeps"]
        return self.generate_ext_config(
            f_ext_config=f_ext_config,
            conn_deps=conn_deps,
        )

    async def add(self, payload: IAddFExtConfig):
        new_record: FExtConfig = {
            "nameLabel": payload["nameLabel"],
            "nameValue": payload["nameValue"],
            "type": payload["type"],
            "config": payload["config"],
        }
        conn_deps: Dict[str, FConnConfig] = {}
        new_f_ext_conn_deps: List[FExtConnDep] = []
        for conn_arg_name in payload["connDeps"]:
            f_conn_config_name_value = payload["connDeps"][conn_arg_name]
            f_conn_config = await self.f_conn_config_service.get_by_name_value(name_value=f_conn_config_name_value)
            if f_conn_config is None:
                raise CErrorResponse(
                    http_code=400, status_code=400, message=f"connector config {f_conn_config_name_value} not found"
                )
            conn_deps[conn_arg_name] = f_conn_config
            new_f_ext_conn_deps.append(
                {
                    "fExtgConfigNameValue": new_record["nameValue"],
                    "fConnConfigNameValue": f_conn_config_name_value,
                    "connArgName": conn_arg_name,
                }
            )
        self.generate_ext_config(f_ext_config=new_record, conn_deps=conn_deps)
        new_record = DataUtil.dumps([new_record], self.__class__.JSON_COLUMNS)[0]
        try:
            with self.session_scope():
                await self.postgres_service.insert(repo=FExtConfigRepo, record=new_record, returning=False)
                await self.f_ext_conn_dep_service.add(new_records=new_f_ext_conn_deps, returning=False)
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(http_code=400, status_code=400, message="loader config name value already exists")


F_EXT_CONFIG_SERVICE = FExtConfigService(
    session_scope=ETL_METADATA_SESSION_SCOPE,
    f_conn_config_service=F_CONN_CONFIG_SERVICE,
    f_ext_conn_dep_service=F_EXT_CONN_DEP_SERVICE,
)
