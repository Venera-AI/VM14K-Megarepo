from typing import Annotated, Dict, Literal, Union

from pydantic import Field, SkipValidation
from src.dtos import BaseDTO, GenerateTypeAdapter


class __IBase__(BaseDTO):
    nameLabel: str
    nameValue: str
    config: Annotated[Dict, SkipValidation]


class __IDependNormal__(BaseDTO):
    connConfig: str


class __IAddNormalConfig__(__IBase__):
    type: Literal["huggingFace"]
    connDeps: __IDependNormal__


IAddFLoaderConfig = Annotated[
    Union[__IAddNormalConfig__],
    Field(discriminator="type"),
]

AddFLoaderConfigDTO = GenerateTypeAdapter[IAddFLoaderConfig](IAddFLoaderConfig)
