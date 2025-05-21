from src.consts import EtlMetadataConsts
from src.entities import FLoaderConnDep
from src.query_builders.postgres import BaseQueryBuilder
from src.repositories import PostgresRepo


class FLoaderConnDepRepo(PostgresRepo[FLoaderConnDep]):
    query_builder = BaseQueryBuilder(table="fLoaderConnDeps", schema=EtlMetadataConsts.SCHEMA)
