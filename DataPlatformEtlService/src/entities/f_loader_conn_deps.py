from typing import Any
from typing_extensions import NotRequired

from src.entities import BaseEntity


class FLoaderConnDep(BaseEntity):
    createdAt: NotRequired[Any]
    updatedAt: NotRequired[Any]
    id: NotRequired[Any]
    fLoaderConfigNameValue: NotRequired[Any]
    fconnConfigNameValue: NotRequired[Any]
    connArgName: NotRequired[Any]
