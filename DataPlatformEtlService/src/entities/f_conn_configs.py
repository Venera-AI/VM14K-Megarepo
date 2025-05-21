from typing import Any
from typing_extensions import NotRequired

from src.entities import BaseEntity


class FConnConfig(BaseEntity):
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    id: NotRequired[Any]
    nameLabel: NotRequired[Any]
    nameValue: NotRequired[Any]
    type: NotRequired[Any]
    config: NotRequired[Any]
