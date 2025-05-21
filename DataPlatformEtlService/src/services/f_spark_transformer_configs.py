from sqlalchemy.exc import IntegrityError

from src.common.responses import CErrorResponse
from src.consts.etl_metadata import EtlMetadataConsts
from src.db.sessions import ETL_METADATA_SESSION_SCOPE
from src.dtos.add_f_spark_transformer_configs import IAddFSparkTransformerConfig
from src.dtos.run_f_spark_transformer_configs import IRunFSparkTransformerConfig
from src.processors.spark_transfomer import ISparkTransformerConfig, SparkTransformerConfigDTO, SparkTransformer
from src.repositories import FSparkTransformerConfigRepo
from src.services import PostgresService, F_CONN_CONFIG_SERVICE, FConnConfigService
from src.utils.data import DataUtil
from src.utils.postgres import PostgresErrorUtils


class FSparkTransformerConfigService:
    JSON_COLUMNS = ["srcViewConfig"]

    def __init__(self, session_scope, f_conn_config_service: FConnConfigService):
        self.session_scope = session_scope
        self.postgres_service = PostgresService(session_scope=session_scope)
        self.f_conn_config_service = f_conn_config_service

    async def get_by_name_value(self, name_value: str):
        records = await self.postgres_service.get_by_condition(
            repo=FSparkTransformerConfigRepo,
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

    async def add(self, payload: IAddFSparkTransformerConfig, returning=False):
        # validate config
        new_record = payload.copy()
        f_conn_config_record = await self.f_conn_config_service.get_by_name_value(
            payload["storageFConnConfigNameValue"]
        )
        if f_conn_config_record is None:
            raise CErrorResponse(
                http_code=400, status_code=400, message=f"connector {payload['storageFConnConfigNameValue']} not found"
            )
        config: ISparkTransformerConfig = {
            "storageConnConfig": {"type": f_conn_config_record["type"], "config": f_conn_config_record["config"]},
            "srcViewConfig": payload["srcViewConfig"],
            "srcFolder": payload["srcFolder"],
            "dstFolder": payload["dstFolder"],
            "transformSql": payload["transformSql"],
        }
        config = SparkTransformerConfigDTO.validate_python(config)
        new_record["transformSql"] = config["transformSql"]
        new_record = DataUtil.dumps([new_record], self.__class__.JSON_COLUMNS)[0]
        try:
            return await self.postgres_service.insert(
                repo=FSparkTransformerConfigRepo, record=new_record, returning=returning
            )
        except IntegrityError as e:
            PostgresErrorUtils.raise_if_not_unique_violation_error(e)
            raise CErrorResponse(
                http_code=400, status_code=400, message="transformation config name value already exists"
            )

    async def get_config_by_name_value(self, name_value: str):
        f_spark_transformer_config_record = await self.get_by_name_value(name_value)
        if f_spark_transformer_config_record is None:
            return None, None
        f_conn_config_record = await self.f_conn_config_service.get_by_name_value(
            f_spark_transformer_config_record["storageFConnConfigNameValue"]
        )
        if f_conn_config_record is None:
            raise CErrorResponse(
                http_code=500,
                status_code=500,
                message=f"connector {f_spark_transformer_config_record['storageFConnConfigNameValue']} not found",
            )
        config: ISparkTransformerConfig = {
            "storageConnConfig": {"type": f_conn_config_record["type"], "config": f_conn_config_record["config"]},
            "srcViewConfig": f_spark_transformer_config_record["srcViewConfig"],
            "srcFolder": f_spark_transformer_config_record["srcFolder"],
            "dstFolder": f_spark_transformer_config_record["dstFolder"],
            "transformSql": f_spark_transformer_config_record["transformSql"],
        }
        config = SparkTransformerConfigDTO.validate_python(config)
        return f_spark_transformer_config_record, config

    async def run_transform(self, payload: IRunFSparkTransformerConfig):
        f_spark_transformer_config_record, config = await self.get_config_by_name_value(payload["nameValue"])
        if config is None:
            raise CErrorResponse(
                http_code=400, status_code=400, message=f"transformation config {payload['nameValue']} not found"
            )
        transformation = SparkTransformer(config, EtlMetadataConsts.SPARK_DNS, f_spark_transformer_config_record["id"])
        transformation.transform(payload["executionTime"])


F_SPARK_TRANSFORMATION_CONFIG_SERVICE = FSparkTransformerConfigService(
    session_scope=ETL_METADATA_SESSION_SCOPE, f_conn_config_service=F_CONN_CONFIG_SERVICE
)
