from typing import Any
from src.dtos import BaseDTO


class IBaseExtConfig(BaseDTO):
    type: Any
    config: Any
