from src.consts import EtlMetadataConsts
from src.entities import FExtConfig
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FExtConfigRepo(PostgresRepo[FExtConfig]):
    query_builder = BaseQueryBuilder(table="fExtConfigs", schema=EtlMetadataConsts.SCHEMA)
