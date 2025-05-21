import sqlparse
from typing import Annotated, List, Literal, NotRequired

from pydantic import AfterValidator
from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IAwsStorageConnConfig


class ISparkViewConfig(BaseDTO):
    alias: str
    path: NotRequired[str]
    pathGlobFilter: NotRequired[str]
    compressType: NotRequired[Literal["bzip2"]]
    fileType: Literal["json"]
    filenameField: NotRequired[str]
    recursiveFileLookup: bool


class ISparkTransformerConfig(BaseDTO):
    storageConnConfig: IAwsStorageConnConfig
    srcViewConfig: List[ISparkViewConfig]
    srcFolder: str
    dstFolder: str
    transformSql: Annotated[str, AfterValidator(lambda v: sqlparse.format(v.strip(), reindent=True))]


SparkTransformerConfigDTO = GenerateTypeAdapter[ISparkTransformerConfig](ISparkTransformerConfig)
