from fastapi import APIRouter
from src.common.consts import MessageConsts
from src.common.responses import CResponse
from src.dtos import IAddFConnConfig, AddFConnConfigDTO
from src.services import F_CONN_CONFIG_SERVICE

ROUTER = APIRouter()


# @ROUTER.post("/")
async def add_f_connector_config(payload: IAddFConnConfig):
    payload = AddFConnConfigDTO.validate_python(payload)
    await F_CONN_CONFIG_SERVICE.add(payload=payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code
