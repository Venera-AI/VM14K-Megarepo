from typing import Annotated, Dict, Literal

from pydantic import Field, SkipValidation
from src.dtos import BaseDTO, GenerateTypeAdapter


class __IBase__(BaseDTO):
    nameLabel: str
    nameValue: str
    config: Annotated[Dict, SkipValidation]


class __IDependNormal__(BaseDTO):
    connConfig: str


class __IAddNormalConfig__(__IBase__):
    type: Literal["postgres", "awsStorage"]
    connDeps: __IDependNormal__


IAddFExtConfig = Annotated[
    __IAddNormalConfig__,
    Field(discriminator="type"),
]

AddFExtConfigDTO = GenerateTypeAdapter[IAddFExtConfig](IAddFExtConfig)
