from typing import Dict
from fastapi import APIRouter
from src.common.consts import MessageConsts
from src.common.responses import CResponse
from src.dtos import AddFSparkTransformerConfigDTO
from src.dtos.run_f_spark_transformer_configs import RunFSparkTransformerConfigDTO
from src.services import F_SPARK_TRANSFORMATION_CONFIG_SERVICE

ROUTER = APIRouter()


# @ROUTER.post("/")
async def add_f_spark_transformer_config(payload: Dict):
    parsed_payload = AddFSparkTransformerConfigDTO.validate_python(payload)
    await F_SPARK_TRANSFORMATION_CONFIG_SERVICE.add(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code


@ROUTER.post("/run")
async def run_f_spark_transformer_config(payload: Dict):
    parsed_payload = RunFSparkTransformerConfigDTO.validate_python(payload)
    await F_SPARK_TRANSFORMATION_CONFIG_SERVICE.run_transform(payload=parsed_payload)
    response = CResponse(
        http_code=201,
        status_code=201,
        message=MessageConsts.CREATED,
    )
    return response.to_dict(), response.http_code
