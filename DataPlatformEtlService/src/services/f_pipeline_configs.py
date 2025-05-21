from typing import cast
from src.common.responses import CErrorResponse
from src.db.sessions.etl_metadata import ETL_METADATA_SESSION_SCOPE
from src.dtos import IAddFPipelineConfig, IRunFPipelineConfig
from src.entities import FPipelineConfig
from src.processors.ext_configs import IExtConfig
from src.processors.loader_configs import ILoaderConfig
from src.processors.pipelines import IPipelineConfig
from src.processors.pipelines.pipelines import Pipeline
from src.repositories import FPipelineConfigRepo
from src.services import (
    PostgresService,
    F_EXT_CONFIG_SERVICE,
    F_LOADER_CONFIG_SERVICE,
    FExtConfigService,
    FLoaderConfigService,
)
from src.utils.logger import LOGGER


class FPipelineConfigService:
    JSON_COLUMNS = ["config"]

    def __init__(
        self, session_scope, f_ext_config_service: FExtConfigService, f_loader_config_service: FLoaderConfigService
    ):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)
        self.f_ext_config_service = f_ext_config_service
        self.f_loader_config_service = f_loader_config_service

    async def get_by_name_value(self, name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FPipelineConfigRepo,
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

    def generate_pipeline_config(
        self, f_pipeline_config: FPipelineConfig, ext_config: IExtConfig, loader_config: ILoaderConfig
    ):
        pipeline_config: IPipelineConfig = {
            "extConfig": ext_config,
            "loaderConfig": loader_config,
            "transferMode": f_pipeline_config["transferMode"],
            "transformSql": f_pipeline_config.get("transformSql", None),
        }
        LOGGER.info(f"pipeline_config: {pipeline_config}")
        return Pipeline.validate_root_config(pipeline_config)

    async def get_pipeline_config(self, f_pipeline_config: FPipelineConfig):
        ext_config = await self.f_ext_config_service.get_ext_config(name_value=f_pipeline_config["fExtConfigNameValue"])
        if ext_config is None:
            raise ValueError(f"ext {f_pipeline_config['fExtConfigNameValue']} not found")
        loader_config = await self.f_loader_config_service.get_loader_config(
            name_value=f_pipeline_config["fLoaderConfigNameValue"]
        )
        if loader_config is None:
            raise ValueError(f"loader {f_pipeline_config['fLoaderConfigNameValue']} not found")
        return self.generate_pipeline_config(
            f_pipeline_config=f_pipeline_config,
            ext_config=ext_config,
            loader_config=loader_config,
        )

    async def add(self, payload: IAddFPipelineConfig):
        new_record = cast(FPipelineConfig, payload.copy())
        await self.get_pipeline_config(f_pipeline_config=new_record)
        await self.postgres_service.insert(repo=FPipelineConfigRepo, record=new_record, returning=False)

    async def run_pipeline(self, payload: IRunFPipelineConfig):
        f_pipeline_config = await self.get_by_name_value(name_value=payload["nameValue"])
        if f_pipeline_config is None:
            raise CErrorResponse(http_code=400, status_code=400, message=f"pipeline {payload['nameValue']} not found")
        pipeline_config = await self.get_pipeline_config(f_pipeline_config=f_pipeline_config)
        if pipeline_config is None:
            raise CErrorResponse(http_code=400, status_code=400, message=f"pipeline {payload['nameValue']} not found")
        pipeline = Pipeline(
            config=pipeline_config,
            name=f_pipeline_config["nameValue"],
            execution_time=payload["executionTime"],
            loader_callback=None,
        )
        await pipeline.run()


F_PIPELINE_CONFIG_SERVICE = FPipelineConfigService(
    session_scope=ETL_METADATA_SESSION_SCOPE,
    f_ext_config_service=F_EXT_CONFIG_SERVICE,
    f_loader_config_service=F_LOADER_CONFIG_SERVICE,
)
