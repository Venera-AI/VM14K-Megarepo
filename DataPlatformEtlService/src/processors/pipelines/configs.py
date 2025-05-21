from typing import Annotated, Any, List, Literal, Optional

from pydantic import AfterValidator, Field
import sqlparse
from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.common import ProcessCommonConsts
from src.processors.ext_configs import IExtConfig
from src.processors.loader_configs import ILoaderConfig


class __IPCBase__(BaseDTO):
    extConfig: IExtConfig
    loaderConfig: ILoaderConfig


class __IPCCopyFile__(__IPCBase__):
    transferMode: Literal["copyFile"]
    transformSql: Any


class __IPCArrowTable__(__IPCBase__):
    transferMode: Literal["arrowTable"]
    transformSql: Annotated[
        Optional[str], AfterValidator(lambda v: sqlparse.format(v.strip(), reindent=True) if v is not None else None)
    ]
    privateFieldPrefix: Annotated[Optional[str], Field(default=ProcessCommonConsts.PRIVATE_FIELD_PREFIX)]
    includePrivateFields: Optional[List[Literal["incrementalValue", "extNameValue", "pipelineNameValue"]]]
    tagNameValue: Optional[str]


IPipelineConfig = Annotated[__IPCCopyFile__ | __IPCArrowTable__, Field(discriminator="transferMode")]


PipelineConfigDTO = GenerateTypeAdapter[IPipelineConfig](IPipelineConfig)
