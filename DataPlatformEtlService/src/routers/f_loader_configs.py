from typing import Any, Dict, cast
from fastapi import APIRouter
from src.common.consts import MessageConsts
from src.common.responses import CResponse
from src.dtos import AddFLoaderConfigDTO
from src.services import F_LOADER_CONFIG_SERVICE

ROUTER = APIRouter()


# @ROUTER.post("/")
async def add_f_loader_config(payload: Dict):
    parsed_payload = AddFLoaderConfigDTO.validate_python(cast(Any, payload))
    await F_LOADER_CONFIG_SERVICE.add(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code
