import multiprocessing
from pyarrow import Table
from typing import Any, List, Literal, NotRequired, Optional

from src.dtos import BaseDTO


class ProcessCommonConsts:
    PRIVATE_FIELD_PREFIX = "__etl_private_field__"
    TRANSFOMER_DUCKDB_TABLE_NAME = "data"


class __IMetadataBase__(BaseDTO):
    incrementalValue: Table
    extNameValue: Table
    pipelineNameValue: Table
    tagNameValue: Optional[Table]


class __IMetadataFile__(__IMetadataBase__):
    path: Table
    prefix: Table


class __IMetadataArrowTable__(__IMetadataBase__):
    incrementalValues: Table
    incrementalField: str


class __IDBPBase__(BaseDTO):
    isEnd: bool


class __IDPBFile__(__IDBPBase__):
    metadata: NotRequired[__IMetadataFile__]
    data: List[bytes]
    dataType: Literal["file"]


class __IDPBArrowTable__(__IDBPBase__):
    metadata: NotRequired[__IMetadataArrowTable__]
    data: Table
    dataType: Literal["arrowTable"]


IProcessDataBlock = __IDPBFile__ | __IDPBArrowTable__


class ProcessState(BaseDTO):
    extIsEnd: Literal[True, False]
    transformerIsEnd: Literal[True, False]
    loaderIsEnd: Literal[True, False]
    extErrorMessage: NotRequired[str]
    transformerErrorMessage: NotRequired[str]
    loaderErrorMessage: NotRequired[str]


class ProcessSharedMemory(BaseDTO):
    extDataStacks: multiprocessing.Queue
    loaderDataStacks: multiprocessing.Queue
    processState: ProcessState
    lock: Any
