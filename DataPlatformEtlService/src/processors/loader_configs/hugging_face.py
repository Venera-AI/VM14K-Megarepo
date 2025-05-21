from typing import Literal
from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IHuggingFaceConnConfig
from src.processors.loader_configs import IBaseLoaderConfig


class _IConfig__(BaseDTO):
    connConfig: IHuggingFaceConnConfig
    repoID: str
    repoType: str
    folder: str
    # batchSize: int


class IHuggingFaceLoaderConfig(IBaseLoaderConfig):
    type: Literal["huggingFace"]
    config: _IConfig__


HuggingfaceLoaderConfigDTO = GenerateTypeAdapter[IHuggingFaceLoaderConfig](IHuggingFaceLoaderConfig)
