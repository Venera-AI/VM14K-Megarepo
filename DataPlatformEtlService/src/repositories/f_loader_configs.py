from src.consts import EtlMetadataConsts
from src.entities import FLoaderConfig
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FLoaderConfigRepo(PostgresRepo[FLoaderConfig]):
    query_builder = BaseQueryBuilder(table="fLoaderConfigs", schema=EtlMetadataConsts.SCHEMA)
