from typing import Any
from typing_extensions import NotRequired

from src.entities import BaseEntity


class FLoaderConfig(BaseEntity):
    id: NotRequired[Any]
    nameLabel: NotRequired[Any]
    nameValue: NotRequired[Any]
    type: NotRequired[Any]
    config: NotRequired[Any]
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    deletedAt: NotRequired[Any]
