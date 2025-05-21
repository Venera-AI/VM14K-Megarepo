from datetime import datetime
from typing import Annotated, Optional

from pydantic import PlainValidator
from src.common.consts import CommonConsts
from src.dtos import BaseDTO, GenerateTypeAdapter
from src.utils.validator import Validator


class IRunFPipelineConfig(BaseDTO):
    nameValue: str
    executionTime: Annotated[
        Optional[datetime],
        PlainValidator(lambda v: v if v is None else Validator.validate_datetime(CommonConsts.TIME_FORMAT, v, True)),
    ]


RunFPipelineConfigDTO = GenerateTypeAdapter[IRunFPipelineConfig](IRunFPipelineConfig)
