from typing import Any, Dict, TypedDict
from typing_extensions import NotRequired
from src.modules.processors.base import ProcessDataBlockInterface


class __Metadata__(TypedDict):
    cdcRecord: Dict[str, Any]


class GcpStorageCdcDBI(ProcessDataBlockInterface):
    metadata: NotRequired[__Metadata__]
