from typing import Any

from src.dtos.base import BaseDTO


class IBaseConnConfig(BaseDTO):
    type: Any
    config: Any
