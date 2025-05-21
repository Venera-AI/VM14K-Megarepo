from typing import List, Literal, Union
from typing_extensions import NotRequired
from src.modules.base.dto import BaseDTO
from src.processors.connectorConfigs import GcpStorageConnectorConfig
from src.processors.connectorConfigs.postgres import PostgresConnectorConfig

databae_connector_types = ["postgres"]


class __FileInfo__(BaseDTO):
    type: Literal["zip"]
    selectRegexOrList: Union[str, List[str]]


class __BaseInterface__(BaseDTO):
    gcpConnectorConfig: GcpStorageConnectorConfig
    cdcSchema: str
    cdcTable: str
    cdcObjectNameField: str
    cdcBucketNameField: str
    filterSql: NotRequired[str]
    batchSize: int
    incrementalField: str
    orderSql: str
    fileInfo: __FileInfo__
    databaseType: Literal["postgres"]


class __PostgresInterface__(__BaseInterface__):
    databaseConnectorConfig: PostgresConnectorConfig


def __get_discriminator_value__(v):
    v = v.get("type", None)
    if v is None or v not in database_con:
        return f"{PYDANTIC_DISCRIMINATOR_KEY}NULL"
    return f"{PYDANTIC_DISCRIMINATOR_KEY}{v}"


GcpStorageCdcConfigInterface = Annotated[
    Union[
        Annotated[__BaseInterface__, Tag(f"{PYDANTIC_DISCRIMINATOR_KEY}NULL")],
        Annotated[
            __GcpStorageInterface__, Tag(f"{PYDANTIC_DISCRIMINATOR_KEY}{TapConfigTypeEnum.GCP_STORAGE_CDC.value}")
        ],
        Annotated[__PostgresInterface__, Tag(f"{PYDANTIC_DISCRIMINATOR_KEY}{TapConfigTypeEnum.POSTGRES.value}")],
        Annotated[__MongoInterface__, Tag(f"{PYDANTIC_DISCRIMINATOR_KEY}{TapConfigTypeEnum.MONGO.value}")],
        Annotated[__UnsedInterface__, Tag(f"{PYDANTIC_DISCRIMINATOR_KEY}UNUSED_NULL")],
    ],
    Discriminator(__get_discriminator_value__),
]


TapConfigDTO = GenerateTypeAdapter[TapConfigInterface](TapConfigInterface)
