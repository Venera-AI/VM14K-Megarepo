from typing import Generic, TypeVar
from typing_extensions import TypedDict
from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass


PYDANTIC_DISCRIMINATOR_KEY = "[__BACKEND_PYDANTIC_DICRIMINATOR__]."

T = TypeVar("T")


class GenerateTypeAdapter(Generic[T]):
    def __new__(cls, type):
        dto = TypeAdapter[T](type=type)
        return dto


class __Config__:
    use_enum_values = True
    extra = "forbid"


@dataclass(config=__Config__)
class BaseDTO(TypedDict):
    pass
