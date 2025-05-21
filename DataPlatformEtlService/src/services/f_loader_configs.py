from typing import Dict
from sqlalchemy.exc import IntegrityError

from src.common.responses import CErrorResponse
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.dtos import IAddFLoaderConfig
from src.entities.f_conn_configs import FConnConfig
from src.entities.f_loader_configs import FLoaderConfig
from src.processors.loader_configs import ILoaderConfig
from src.processors.loaders import get_loader_class
from src.query_builders.postgres import SqlQuery
from src.repositories import FLoaderConfigRepo
from src.repositories.f_conn_configs import FConnConfigRepo
from src.repositories.f_loader_conn_deps import FLoaderConnDepRepo
from src.repositories.postgres import PostgresRepo
from src.services import PostgresService, F_CONN_CONFIG_SERVICE, FLoaderConnDepService, F_LOADER_CONN_DEP_SERVICE
from src.utils.data import DataUtil
from src.utils.postgres import PostgresErrorUtils


class FLoaderConfigService:
    JSON_COLUMNS = ["config"]

    def __init__(self, session_scope, f_conn_service, f_loader_conn_dep_service: FLoaderConnDepService):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)
        self.f_conn_service = f_conn_service
        self.f_loader_conn_dep_service = f_loader_conn_dep_service

    async def get_by_name_value(self, name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FLoaderConfigRepo,
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

    def generate_loader_config(self, f_loader_config: FLoaderConfig, conn_deps: Dict[str, FConnConfig]):
        loader_config: ILoaderConfig = {"type": f_loader_config["type"], "config": f_loader_config["config"].copy()}
        for conn_arg_name in conn_deps:
            conn_config = self.f_conn_service.generate_conn_config(conn_deps[conn_arg_name])
            loader_config["config"][conn_arg_name] = conn_config
        loader_class = get_loader_class(loader_config)
        return loader_class.validate_root_config(loader_config)

    async def get_loader_config(self, name_value: str):
        sql = f"""
            SELECT flc.*, JSON_AGG(JSON_BUILD_OBJECT('connArgName', flcd."connArgName",'config',fcc.*)) AS "connDeps"
            FROM {FLoaderConfigRepo.query_builder.full_table_name} flc
            LEFT JOIN {FLoaderConnDepRepo.query_builder.full_table_name} flcd ON
                flc."nameValue"=flcd."fLoaderConfigNameValue"
            LEFT JOIN {FConnConfigRepo.query_builder.full_table_name} fcc ON
                fcc."nameValue"=flcd."fConnConfigNameValue"
            WHERE flc."nameValue" = %s
            GROUP BY flc."id"
        """
        query = SqlQuery(sql=sql, params=[name_value])
        records = PostgresRepo.row_factory(await self.postgres_service.execute_raw_query(query.sql, query.params))
        if len(records) == 0:
            return None
        record = records[0]
        conn_deps: Dict[str, FConnConfig] = {
            conn_dep["connArgName"]: conn_dep["config"] for conn_dep in record["connDeps"]
        }
        f_loader_config = record.copy()
        del f_loader_config["connDeps"]
        return self.generate_loader_config(
            f_loader_config=f_loader_config,
            conn_deps=conn_deps,
        )

    async def add(self, payload: IAddFLoaderConfig):
        new_record: FLoaderConfig = {
            "nameLabel": payload["nameLabel"],
            "nameValue": payload["nameValue"],
            "type": payload["type"],
            "config": payload["config"],
        }
        conn_deps: Dict[str, FConnConfig] = {}
        new_f_loader_conn_deps = []
        for conn_arg_name in payload["connDeps"]:
            f_conn_config_name_value = payload["connDeps"][conn_arg_name]
            f_conn_config = await self.f_conn_service.get_by_name_value(name_value=f_conn_config_name_value)
            if f_conn_config is None:
                raise CErrorResponse(
                    http_code=400, status_code=400, message=f"connector config {f_conn_config_name_value} not found"
                )
            conn_deps[conn_arg_name] = f_conn_config
            new_f_loader_conn_deps.append(
                {
                    "fLoaderConfigNameValue": new_record["nameValue"],
                    "fConnConfigNameValue": f_conn_config_name_value,
                    "connArgName": conn_arg_name,
                }
            )
        self.generate_loader_config(f_loader_config=new_record, conn_deps=conn_deps)
        new_record = DataUtil.dumps([new_record], self.__class__.JSON_COLUMNS)[0]
        try:
            with self.session_scope():
                await self.postgres_service.insert(repo=FLoaderConfigRepo, record=new_record, returning=False)
                await self.f_loader_conn_dep_service.add(new_records=new_f_loader_conn_deps, returning=False)
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(http_code=400, status_code=400, message="loader config name value already exists")


F_LOADER_CONFIG_SERVICE = FLoaderConfigService(
    session_scope=ETL_METADATA_SESSION_SCOPE,
    f_conn_service=F_CONN_CONFIG_SERVICE,
    f_loader_conn_dep_service=F_LOADER_CONN_DEP_SERVICE,
)
