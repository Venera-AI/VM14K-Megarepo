from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Generic, TypeVar, cast

from src.processors.common import IProcessDataBlock


CONFIG_TYPE = TypeVar("CONFIG_TYPE")


class BaseLoader(Generic[CONFIG_TYPE], metaclass=ABCMeta):
    def __init__(
        self,
        config: CONFIG_TYPE,
        pipeline_name: str,
        execution_time: datetime | None,
    ):
        self.root_config = self.validate_root_config(config)
        self.pipeline_name = pipeline_name
        self.is_enough_batch_size = self.is_raise_crash = self.is_last_block = False
        self.execution_time: datetime | str = cast(Any, execution_time)
        if self.execution_time is None:
            self.execution_time = "*"

    @abstractmethod
    async def push(self, data_block: IProcessDataBlock):
        pass

    @classmethod
    @abstractmethod
    def is_compatible_with_transfer_copy_file(cls) -> bool:
        pass

    @classmethod
    @abstractmethod
    def validate_root_config(cls, config) -> CONFIG_TYPE:
        raise NotImplementedError()
