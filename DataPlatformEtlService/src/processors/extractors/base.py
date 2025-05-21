from abc import ABCMeta, abstractmethod
from typing import Generic, List, TypeVar

from src.processors.interfaces import ProcessDataBlockInterface, ProcessSharedMemory


CONNECTOR_TYPE = TypeVar("CONNECTOR_TYPE")
CONFIG_TYPE = TypeVar("CONFIG_TYPE")


class BaseExt(Generic[CONFIG_TYPE], metaclass=ABCMeta):
    def __init__(
        self,
        config: CONFIG_TYPE,
        process_shared_memory: ProcessSharedMemory,
        init_incremental_value: str,
        private_field_prefix: str,
        selected_fields: List[str],
        incremental_mode: str,
    ):
        self.config = config
        self.process_shared_memory = process_shared_memory
        self.init_incremental_value = init_incremental_value
        self.private_field_prefix = private_field_prefix
        self.selected_fields = selected_fields
        self.incremental_mode = incremental_mode

    @abstractmethod
    async def extract(self) -> List[ProcessDataBlockInterface]:
        pass
