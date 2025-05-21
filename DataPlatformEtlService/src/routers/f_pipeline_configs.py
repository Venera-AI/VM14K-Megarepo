from typing import Dict, cast, Any
from fastapi import APIRouter
from src.common.consts import MessageConsts
from src.common.responses import CResponse
from src.dtos import AddFPipelineConfigDTO, RunFPipelineConfigDTO
from src.services import F_PIPELINE_CONFIG_SERVICE

ROUTER = APIRouter()


# @ROUTER.post("/")
async def add_f_pipeline_config(payload: Dict):
    parsed_payload = AddFPipelineConfigDTO.validate_python(cast(Any, payload))
    await F_PIPELINE_CONFIG_SERVICE.add(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code


@ROUTER.post("/run")
async def trigger_pipeline(payload: Dict):
    parsed_payload = RunFPipelineConfigDTO.validate_python(payload)
    await F_PIPELINE_CONFIG_SERVICE.run_pipeline(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code
