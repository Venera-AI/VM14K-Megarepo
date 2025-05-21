from src.consts import EtlMetadataConsts
from src.entities import FExtConnDep
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FExtConnDepRepo(PostgresRepo[FExtConnDep]):
    query_builder = BaseQueryBuilder(table="fExtConnDeps", schema=EtlMetadataConsts.SCHEMA)
