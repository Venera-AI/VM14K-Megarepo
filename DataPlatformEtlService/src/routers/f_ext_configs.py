from typing import Any, Dict, cast
from fastapi import APIRouter
from src.common.consts import MessageConsts
from src.common.responses import CResponse
from src.dtos import AddFExtConfigDTO
from src.services import F_EXT_CONFIG_SERVICE

ROUTER = APIRouter()


# @ROUTER.post("/")
async def add_f_ext_config(payload: Dict):
    parsed_payload = AddFExtConfigDTO.validate_python(cast(Any, payload))
    await F_EXT_CONFIG_SERVICE.add(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code
