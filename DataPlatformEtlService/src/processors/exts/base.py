from pyarrow import Table
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Dict, Generic, List, TypeVar, cast

from src.processors.common import IProcessDataBlock
from src.processors.ext_configs import IBaseExtConfig


CONFIG_TYPE = TypeVar("CONFIG_TYPE", bound=IBaseExtConfig)


class BaseExt(Generic[CONFIG_TYPE], metaclass=ABCMeta):
    def __init__(
        self,
        config: CONFIG_TYPE,
        execution_time: datetime | None,
    ):
        self.root_config = self.validate_root_config(config)
        self.execution_time: datetime | str = cast(Any, execution_time)
        if self.execution_time is None:
            self.execution_time = "*"

    @abstractmethod
    async def extract(self, init_metadata_values: Dict) -> List[IProcessDataBlock]:
        pass

    @classmethod
    @abstractmethod
    def is_compatible_with_transfer_copy_file(cls) -> bool:
        pass

    @classmethod
    @abstractmethod
    def validate_root_config(cls, config) -> CONFIG_TYPE:
        pass

    def bytes_to_arrow_table(self, data: bytes) -> Table:
        raise NotImplementedError(data)
