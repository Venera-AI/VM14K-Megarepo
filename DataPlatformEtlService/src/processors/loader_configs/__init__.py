from typing import Annotated, Union

from pydantic import Field

from src.dtos.base import GenerateTypeAdapter
from .base import IBaseLoaderConfig
from .hugging_face import IHuggingFaceLoaderConfig, HuggingfaceLoaderConfigDTO

# ILoaderConfig = Annotated[
#     Union[IHuggingFaceLoaderConfig],
#     Field(discriminator="type"),
# ]
ILoaderConfig = IHuggingFaceLoaderConfig

LoaderConfigDTO = GenerateTypeAdapter[ILoaderConfig](ILoaderConfig)
