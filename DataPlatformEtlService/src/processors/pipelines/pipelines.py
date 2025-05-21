import asyncio
import json
import multiprocessing
from datetime import datetime
import time
from typing import Any, Callable, Coroutine, Dict, cast
from src.common.responses.error import CErrorResponse
from src.processors.common import ProcessSharedMemory
from src.processors.ext_configs import ExtConfigDTO
from src.processors.exts import get_ext_class
from src.processors.loader_configs import LoaderConfigDTO
from src.processors.loaders import BaseLoader, get_loader_class
from src.processors.pipelines import IPipelineConfig, PipelineConfigDTO
from src.processors.transfomer import Transformer
from src.utils.logger import LOGGER

TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


async def ext_process_func(
    process_shared_memory: ProcessSharedMemory,
    init_metadata_values: Dict,
    ext_config_string: str,
    execution_time_string: str | None,
):
    def end_process(error_message: str | None = None):
        if error_message is not None:
            process_state["extErrorMessage"] = error_message
        process_state["extIsEnd"] = True
        LOGGER.info("[PIPELINE][EXT] end process")

    process_state = process_shared_memory["processState"]
    ext_data_stacks = process_shared_memory["extDataStacks"]
    lock = process_shared_memory["lock"]
    execution_time = (
        datetime.strptime(execution_time_string, TIME_FORMAT) if execution_time_string is not None else None
    )
    try:
        ext_config = ExtConfigDTO.validate_python(json.loads(ext_config_string))
        ext_class = get_ext_class(ext_config)
        ext = ext_class(
            config=cast(Any, ext_config),
            execution_time=execution_time,
        )
        while True:
            if process_state["loaderIsEnd"] is True:
                end_process()
                break
            data_blocks = await ext.extract(init_metadata_values=init_metadata_values)
            for data_block in data_blocks:
                ext_data_stacks.put(data_block, block=True)
            if data_blocks[-1]["isEnd"] is True:
                end_process()
                return
    except Exception as e:
        with lock:
            end_process(f"{type(e).__name__}: {e}")
        raise e


async def transfomer_process_func(
    process_shared_memory: ProcessSharedMemory,
    pipeline_config_string: str,
    execution_time_string: str,
):
    def end_process(error_message: str | None = None):
        if error_message is not None:
            process_state["transformerErrorMessage"] = error_message
        process_state["transformerIsEnd"] = True
        LOGGER.info("[PIPELINE][TRANSFORMER] end process")

    process_state = process_shared_memory["processState"]
    ext_data_stacks = process_shared_memory["extDataStacks"]
    lock = process_shared_memory["lock"]
    execution_time = (
        datetime.strptime(execution_time_string, TIME_FORMAT) if execution_time_string is not None else None
    )
    error_message = None
    try:
        pipeline_config = PipelineConfigDTO.validate_python(json.loads(pipeline_config_string))
        ext_config = pipeline_config["extConfig"]
        ext_class = get_ext_class(ext_config)
        ext = ext_class(
            config=cast(Any, ext_config),
            execution_time=execution_time,
        )
        transformer = Transformer(pipeline_config=pipeline_config, ext=ext)
        while True:
            try:
                data_block = ext_data_stacks.get_nowait()
            except Exception:
                if process_state["loaderIsEnd"] is True:
                    break
                LOGGER.info("[PIPELINE][TRANSFORMER] wait extractors push more data blocks")
                time.sleep(2)
                continue
            transformer.transform(data_blocks=[data_block])
            if data_block["isEnd"] is True:
                break
    except Exception as e:
        error_message = f"{type(e).__name__}: {e}"
        raise e
    finally:
        with lock:
            end_process(error_message=error_message)


async def loader_process_func(
    process_shared_memory: ProcessSharedMemory,
    loader_config_string: str,
    execution_time_string: str | None,
    pipeline_name: str,
    ext_process: multiprocessing.Process,
    transfomer_process: multiprocessing.Process | None,
    loader_callback: Callable[[BaseLoader], Coroutine] | None,
):
    def end_process(error_message: str | None = None):
        if error_message is not None:
            process_state["loaderErrorMessage"] = error_message
        process_state["loaderIsEnd"] = True
        LOGGER.info("[PIPELINE][LOADER] end process")

    process_state = process_shared_memory["processState"]
    loader_data_stacks = process_shared_memory["loaderDataStacks"]
    lock = process_shared_memory["lock"]
    execution_time = (
        datetime.strptime(execution_time_string, TIME_FORMAT) if execution_time_string is not None else None
    )
    try:
        loader_config = LoaderConfigDTO.validate_python(json.loads(loader_config_string))
        loader_class = get_loader_class(loader_config)
        loader = loader_class(
            config=cast(Any, loader_config),
            pipeline_name=pipeline_name,
            execution_time=execution_time,
        )
        while True:
            try:
                data_block = loader_data_stacks.get_nowait()
            except Exception:
                if ext_process.is_alive() is False | (
                    transfomer_process is not None and transfomer_process.is_alive() is False
                ):
                    break
                LOGGER.info("[PIPELINE][LOADER] wait transformer push more data blocks")
                time.sleep(2)
                continue
            await loader.push(data_block)
            if loader_callback is not None:
                await loader_callback(loader)
            if data_block["isEnd"] is True:
                break
    except Exception as e:
        with lock:
            end_process(f"{type(e).__name__}: {e}")
        raise e


def wrap_async_process_func(f, *args, **kwargs):
    return asyncio.run(f(*args, **kwargs))


class Pipeline:
    def __init__(
        self,
        config: IPipelineConfig,
        name: str,
        execution_time: datetime | None,
        loader_callback: Callable | None,
    ):
        self.root_config = self.validate_root_config(config)
        self.name = name
        self.execution_time = execution_time
        self.shm_manager = multiprocessing.Manager()
        self.process_shared_memory: ProcessSharedMemory = cast(Any, self.init_proccess_shared_memory())
        self.ext_process, self.transfomer_process = self.generate_process()
        self.loader_callback = loader_callback
        self.is_need_transform = self.__validate_need_transform__()

    @classmethod
    def validate_root_config(cls, config):
        config = PipelineConfigDTO.validate_python(config)
        if config["transferMode"] != "copyFile":
            ext_class = get_ext_class(config["extConfig"])
            loader_class = get_loader_class(config["loaderConfig"])
            if not ext_class.is_compatible_with_transfer_copy_file():
                raise ValueError("ext is not compatible with transfer copy file")
            if not loader_class.is_compatible_with_transfer_copy_file():
                raise ValueError("loader is not compatible with transfer copy file")
        elif config["transferMode"] == "arrowTable":
            pass

        return PipelineConfigDTO.validate_python(config)

    def __validate_need_transform__(self):
        transform_sql = self.root_config.get("transformSql", None)
        include_private_fields = self.root_config.get("includePrivateFields", None)
        condition1 = self.root_config["transferMode"] == "arrowTable"
        condition2 = transform_sql
        condition3 = include_private_fields is not None and len(include_private_fields) > 0
        return condition1 and (condition2 or condition3)

    def init_proccess_shared_memory(self):
        def init_process_state():
            process_state = self.process_shared_memory["processState"]
            process_state["loaderIsEnd"] = False
            process_state["extIsEnd"] = False

        process_shared_memory = self.shm_manager.dict()
        if self.is_need_transform:
            process_shared_memory["extDataStacks"] = self.shm_manager.Queue()
            process_shared_memory["loaderDataStacks"] = self.shm_manager.Queue()
        else:
            data_stacks = self.shm_manager.Queue()
            process_shared_memory["extDataStacks"] = data_stacks
            process_shared_memory["loaderDataStacks"] = data_stacks
        process_shared_memory["processState"] = self.shm_manager.dict()
        process_shared_memory["lock"] = multiprocessing.RLock()
        init_process_state()
        return process_shared_memory

    def generate_process(self):
        ext_process = multiprocessing.Process(
            target=wrap_async_process_func,
            args=(
                ext_process_func,
                self.process_shared_memory,
                json.dumps(self.root_config["extConfig"]),
                self.execution_time.strftime(TIME_FORMAT) if self.execution_time is not None else None,
            ),
            daemon=True,
        )
        if self.is_need_transform:
            transfomer_process = multiprocessing.Process(
                target=wrap_async_process_func,
                args=(
                    transfomer_process_func,
                    self.process_shared_memory,
                    json.dumps(self.root_config),
                    self.execution_time.strftime(TIME_FORMAT) if self.execution_time is not None else None,
                ),
                daemon=True,
            )
        else:
            transfomer_process = None

        return ext_process, transfomer_process

    async def run(self):
        self.ext_process.start()
        if self.transfomer_process is not None:
            self.transfomer_process.start()
        try:
            await loader_process_func(
                process_shared_memory=self.process_shared_memory,
                loader_config_string=json.dumps(self.root_config["loaderConfig"]),
                execution_time_string=(
                    self.execution_time.strftime(TIME_FORMAT) if self.execution_time is not None else None
                ),
                pipeline_name=self.name,
                ext_process=self.ext_process,
                transfomer_process=self.transfomer_process,
                loader_callback=self.loader_callback,
            )
        except Exception as e:
            raise e
        finally:
            self.ext_process.terminate()
            self.ext_process.join()
            if self.ext_process.exitcode != 0:
                raise CErrorResponse(
                    http_code=500,
                    status_code=500,
                    message=(
                        f"ext process exit with code {self.ext_process.exitcode}"
                        ", error: {self.process_shared_memory['processState'].get('extErrorMessage', 'unknown')}"
                    ),
                )
            self.process_shared_memory["processState"]["loaderIsEnd"] = True
