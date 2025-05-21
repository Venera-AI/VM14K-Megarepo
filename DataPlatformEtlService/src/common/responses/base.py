from typing import List, Dict, Union, Any

from src.custom_types.common import BaseDict


class CResponse:
    def __init__(
        self,
        http_code: int,
        status_code: int,
        message: str,
        data: Union[List, BaseDict, Any] = None,
        errors: BaseDict | None = None,
    ):
        self.http_code = http_code
        self.status_code = status_code
        self.message = message
        self.data = data
        self.errors = errors

    def to_dict(self) -> Dict:
        result = {"statusCode": self.status_code, "message": self.message}
        if self.data:
            result["data"] = self.data
        if self.errors:
            result["errors"] = self.errors
        return result
