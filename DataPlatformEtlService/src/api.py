from typing import List

from fastapi import APIRouter
from starlette.responses import JSONResponse

from src.common.consts import MessageConsts
from src.dtos import BaseDTO
from src.routers.f_pipeline_configs import ROUTER as F_PIPELINE_CONFIG_ROUTER


class ErrorDetailModel(BaseDTO):
    field: List[str]


class ErrorResponseModel(BaseDTO):
    statusCode: int
    message: str
    error: ErrorDetailModel


api_router = APIRouter(
    default_response_class=JSONResponse,
    responses={
        400: {"model": ErrorResponseModel},
        401: {"model": ErrorResponseModel},
        422: {"model": ErrorResponseModel},
        500: {"model": ErrorResponseModel},
    },
)

# api_router.include_router(F_TRANSFORMATION_CONFIG_ROUTER, prefix="/fTransformationConfigs", tags=["Transformations"])
api_router.include_router(F_PIPELINE_CONFIG_ROUTER, prefix="/fPipelineConfigs", tags=["Pipelines"])


@api_router.get("/healthcheck", include_in_schema=False)
def healthcheck():
    return JSONResponse(status_code=200, content={"message": MessageConsts.SUCCESS})
