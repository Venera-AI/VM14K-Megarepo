from .base import BaseExt, OrderFieldInterface, FilterInterface
from .gcp_storage_cdc import GcpStorageCdcExtConfig, GcpStorageCdcExt, GcpStorageCdcDBI
from .postgres import PostgresExt, PostgresExtConfig
from .mongo import MongoExt, MongoExtConfig

EXTRACTORS = {
    "gcp_storage_cdc": GcpStorageCdcExt,
    "postgres": PostgresExt,
    "mongo": MongoExt,
}
