from typing import Any
from typing_extensions import NotRequired

from src.entities import BaseEntity


class FExtConnDep(BaseEntity):
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    id: NotRequired[Any]
    fExtgConfigNameValue: NotRequired[Any]
    fConnConfigNameValue: NotRequired[Any]
    connArgName: NotRequired[Any]
