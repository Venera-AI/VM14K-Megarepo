from typing import Any

from src.dtos import BaseDTO


class IBaseLoaderConfig(BaseDTO):
    type: Any
    config: Any
