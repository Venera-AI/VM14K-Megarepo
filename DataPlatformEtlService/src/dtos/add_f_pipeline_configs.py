from typing import Annotated, Any, NotRequired

from pydantic import SkipValidation

from src.dtos import BaseDTO, GenerateTypeAdapter


class IAddFPipelineConfig(BaseDTO):
    nameLabel: str
    nameValue: str
    fExtConfigNameValue: str
    fLoaderConfigNameValue: str
    transferMode: Annotated[Any, SkipValidation]
    transformSql: Annotated[NotRequired[Any], SkipValidation]
    privateFieldPrefix: Annotated[NotRequired[Any], SkipValidation]
    includePrivateFields: Annotated[NotRequired[Any], SkipValidation]
    tagNameValue: Annotated[NotRequired[Any], SkipValidation]


AddFPipelineConfigDTO = GenerateTypeAdapter[IAddFPipelineConfig](IAddFPipelineConfig)
