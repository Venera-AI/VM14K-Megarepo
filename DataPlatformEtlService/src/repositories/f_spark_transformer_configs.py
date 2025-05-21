from src.consts import EtlMetadataConsts
from src.entities import FSparkTransformerConfig
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FSparkTransformerConfigRepo(PostgresRepo[FSparkTransformerConfig]):
    query_builder = BaseQueryBuilder(table="fSparkTransformerConfigs", schema=EtlMetadataConsts.SCHEMA)
