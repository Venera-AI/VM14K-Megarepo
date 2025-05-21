from typing import Annotated, Any

from pydantic import SkipValidation
from src.dtos import BaseDTO, GenerateTypeAdapter


class IAddFSparkTransformerConfig(BaseDTO):
    nameLabel: str
    nameValue: str
    storageFConnConfigNameValue: str
    srcViewConfig: Annotated[Any, SkipValidation]
    srcFolder: Annotated[Any, SkipValidation]
    dstFolder: Annotated[Any, SkipValidation]
    transformSql: Annotated[Any, SkipValidation]


AddFSparkTransformerConfigDTO = GenerateTypeAdapter[IAddFSparkTransformerConfig](IAddFSparkTransformerConfig)
