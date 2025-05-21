from typing import Any, List, Union
from src.common.responses import CResponse
from src.custom_types.common import BaseDict


class CErrorResponse(CResponse, Exception):
    def __init__(
        self, http_code, status_code, message, data: Union[List, BaseDict, Any] = None, errors: BaseDict | None = None
    ):
        super(Exception, self).__init__(message)
        super().__init__(http_code=http_code, status_code=status_code, message=message, data=data, errors=errors)
