from typing import Any
from typing_extensions import NotRequired
from src.entities import BaseEntity


class FPipelineConfig(BaseEntity):
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    id: NotRequired[Any]
    nameLabel: NotRequired[Any]
    nameValue: NotRequired[Any]
    fExtConfigNameValue: NotRequired[Any]
    fLoaderConfigNameValue: NotRequired[Any]
    transferMode: NotRequired[Any]
    transformSql: NotRequired[Any]
