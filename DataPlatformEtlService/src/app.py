import json
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

from src.api import api_router
from src.common.consts import MessageConsts, CommonConsts
from src.common.responses import CErrorResponse
from src.utils.logger import LOGGER

app = FastAPI(
    title="backend",
    description="Welcome to API documentation",
    # root_path="/api/v1",
    docs_url="/docs" if CommonConsts.DEBUG else None,
    # openapi_url="/docs/openapi.json",
    redoc_url="/docs" if CommonConsts.DEBUG else None,
)
cors = CORSMiddleware(app, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(RequestValidationError)
async def exception_handler_pydantic(request: Request, exception: RequestValidationError):
    errors = {}
    for error in exception.errors():
        field = []
        _errors = errors
        mssg_list = None
        if len(error["loc"]) == 1:
            if "ctx" in error and "discriminator_key" in error["ctx"]:
                field = error["ctx"]["discriminator_key"]
                if field not in _errors:
                    _errors[field] = []
                _errors[field].append(error["msg"])
                continue
        for i in range(1, len(error["loc"])):
            field = error["loc"][i]
            if field not in _errors:
                _errors[field] = [] if i == len(error["loc"]) - 1 else {}
                if isinstance(errors[field], (tuple, list)):
                    mssg_list = _errors[field]
            _errors = _errors[field]
        if mssg_list is not None:
            mssg_list.append(error["msg"])  # type: ignore
    error_response = CErrorResponse(
        http_code=400,
        status_code=400,
        message=MessageConsts.BAD_REQUEST,
        errors=errors,
    )
    error_dict = error_response.to_dict()
    LOGGER.error(json.dumps(error_dict))
    return JSONResponse(
        status_code=error_response.http_code,
        content=error_dict,
    )


@app.exception_handler(Exception)
async def exception_handler_system(request: Request, exception):
    if isinstance(exception, CErrorResponse):
        error_response = exception.to_dict()
    else:
        errors = (
            None if not CommonConsts.DEBUG else {"key": MessageConsts.INTERNAL_SERVER_ERROR, "message": str(exception)}
        )
        exception = CErrorResponse(
            http_code=500,
            status_code=500,
            message=MessageConsts.INTERNAL_SERVER_ERROR,
            errors=errors,
        )
        error_response = exception.to_dict()
    LOGGER.error(json.dumps(error_response))
    return JSONResponse(
        status_code=exception.http_code,
        content=error_response,
    )


app.include_router(router=api_router)
