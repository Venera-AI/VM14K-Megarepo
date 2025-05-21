from src.consts import EtlMetadataConsts
from src.entities import FConnConfig
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FConnConfigRepo(PostgresRepo[FConnConfig]):
    query_builder = BaseQueryBuilder(table="fConnConfigs", schema=EtlMetadataConsts.SCHEMA)
