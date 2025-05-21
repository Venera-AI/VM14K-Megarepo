from typing import Any, Dict, List, Union

from src.common.responses import CResponse
from src.custom_types.common import BaseDict


class CPaginationResponse(CResponse):
    def __init__(self, http_code, status_code, message, data: Union[List, BaseDict, Any], page, page_size, total):
        super().__init__(http_code=http_code, status_code=status_code, message=message, data=data)
        self.page = page
        self.page_size = page_size
        self.total = total

    def to_dict(self) -> Dict:
        result = super().to_dict()
        if self.page is not None:
            result["page"] = self.page
        if self.page_size is not None:
            result["pageSize"] = self.page_size
        if self.total is not None:
            result["total"] = self.total
        return result
