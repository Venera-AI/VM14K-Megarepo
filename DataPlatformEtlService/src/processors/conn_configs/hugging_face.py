from typing import Annotated, Literal

from pydantic import Field
from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    accessToken: str


class IHuggingFaceConnConfig(IBaseConnConfig):
    type: Annotated[Literal["huggingFace"], Field(default="huggingFace", validate_default=True)]
    config: __IConfig__


HuggingFaceConnConfigDTO = GenerateTypeAdapter[IHuggingFaceConnConfig](IHuggingFaceConnConfig)
