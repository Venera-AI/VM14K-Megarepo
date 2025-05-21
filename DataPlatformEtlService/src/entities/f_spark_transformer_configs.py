from typing import Any
from typing_extensions import NotRequired

from src.entities import BaseEntity


class FSparkTransformerConfig(BaseEntity):
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    id: NotRequired[Any]
    nameLabel: NotRequired[Any]
    nameValue: NotRequired[Any]
    storageFConnConfigNameValue: NotRequired[Any]
    srcViewConfig: NotRequired[Any]
    srcFolder: NotRequired[Any]
    dstFolder: NotRequired[Any]
    transformSql: NotRequired[Any]
    deletedAt: NotRequired[Any]
