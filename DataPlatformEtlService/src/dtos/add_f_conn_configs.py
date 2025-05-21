from typing import Annotated, Any

from pydantic import SkipValidation
from src.dtos import BaseDTO, GenerateTypeAdapter


class IAddFConnConfig(BaseDTO):
    nameLabel: str
    nameValue: str
    type: Annotated[str, SkipValidation]
    config: Annotated[Any, SkipValidation]


AddFConnConfigDTO = GenerateTypeAdapter[IAddFConnConfig](IAddFConnConfig)
