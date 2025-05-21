from src.consts import EtlMetadataConsts
from src.entities import FPipelineConfig
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FPipelineConfigRepo(PostgresRepo[FPipelineConfig]):
    query_builder = BaseQueryBuilder(table="fPipelineConfigs", schema=EtlMetadataConsts.SCHEMA)
