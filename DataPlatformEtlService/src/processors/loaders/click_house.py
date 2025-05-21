from queue import Empty
import time
import uuid

from huggingface_hub import HfApi
from src.db.sessions.base import BaseSession
from src.processors.conns import HuggingFaceConnPool
from src.processors.loader_configs import IHuggingFaceLoaderConfig, HuggingfaceLoaderConfigDTO
from src.processors.loaders import BaseLoader
from src.utils.data_file import DataFileUtil
from src.utils.logger import LOGGER


class ClickHouseLoader(BaseLoader[IHuggingFaceLoaderConfig]):
    def __init__(
        self,
        config,
        pipeline_name,
        process_shared_memory,
        execution_time,
    ):
        super().__init__(
            config=config,
            pipeline_name=pipeline_name,
            process_shared_memory=process_shared_memory,
            execution_time=execution_time,
        )
        self.config = self.root_config["config"]
        self.data_pool = []
        self.wait_push_data = []
        self.first_run = True
        self.connector = BaseSession[HfApi](
            inputs=self.config["connectorConfig"], pool_class=HuggingFaceConnPool
        ).generate_session_scope_func()

    @classmethod
    def is_compatible_with_transfer_copy_file(cls):
        return False

    @classmethod
    def validate_root_config(cls, config):
        return HuggingfaceLoaderConfigDTO.validate_python(config)

    async def __put_data__(self, file_name, file_type, records):
        # file_name = f"IV_{incremental_value}_PAGE_{page}.{file_type}"
        if file_type == "csv":
            file = await DataFileUtil.dict_to_csv_buffer(records)
        elif file_type == "json":
            file = await DataFileUtil.dict_to_buffer(records)
        else:
            raise ValueError(f"file type {file_type} not exists")
        with self.connector() as api:
            api.upload_file(
                path_or_fileobj=file,
                path_in_repo=file_name,
                repo_id=self.config["repoID"],
                repo_type=self.config["repoType"],
            )

    async def push(self):
        queue = self.process_shared_memory["dataQueue"]
        process_state = self.process_shared_memory["processState"]
        tap_is_done = process_state["tapIsDone"]
        try:
            data_block = queue.get_nowait()
            if data_block["isEnd"] is True:
                self.is_last_block = True
            self.data_pool += data_block["data"]
        except Empty:
            if tap_is_done is False:
                LOGGER.info("[PIPELINE][LOADER] wait extractors push more data blocks")
                LOGGER.info("[PIPELINE][LOADER] current batch size: %s" % len(self.data_pool))
                time.sleep(1)
                return
            else:
                self.is_last_block = True
        self.is_enough_batch_size = len(self.data_pool) >= self.config["batchSize"]
        if (self.is_enough_batch_size or self.is_last_block) and len(self.data_pool) > 0:
            # push data
            # Prepare file name first
            file_type = self.config["fileType"]
            file_name_prefix = "ETL_%s_IV_%s_" % (
                self.etl_id,
                "" if self.current_incremental_value is None else self.current_incremental_value,
            )
            # if self.first_run and self.root_config["pushMode"] == "dedup":
            #     await self.deduped(file_name_prefix)
            #     self.first_run = False
            file_name = f"{file_name_prefix}{str(uuid.uuid4())}.{file_type}"
            # if self.root_config["pushMode"] == "dedup":
            #     last_incremental_value = self.data_pool[-1][self.private_incremental_field]
            #     # day data tu data_pool vao wait_push_data
            #     for data_id in range(len(self.data_pool)):
            #         if self.data_pool[-data_id - 1][self.private_incremental_field] != last_incremental_value:
            #             self.wait_push_data = self.data_pool[:-data_id]
            #             self.data_pool = self.data_pool[-data_id:]
            #             self.current_incremental_value = self.wait_push_data[-1][self.private_incremental_field]
            #             break
            # else:
            self.wait_push_data = self.data_pool
            self.data_pool = []
            self.current_incremental_value = self.wait_push_data[-1][self.private_incremental_field]
            # upload toan bo data neu k split duoc (tat ca incremental_value bang nhau)
            if len(self.wait_push_data) == 0:
                self.wait_push_data = self.data_pool
                self.data_pool = []
            await self.__put_data__(file_name=file_name, file_type=file_type, records=self.wait_push_data)
            self.is_upload = False
            self.wait_push_data = []
            if self.is_last_block:
                self.process_shared_memory["processState"]["targetIsDone"] = True
            return self.current_incremental_value
        return

    async def deduped(self, file_name_prefix):
        with self.connector() as api:
            files = api.list_repo_files(
                repo_id=self.config["repoID"],
                repo_type=self.config["repoType"],
            )
            for file in files:
                # 36 là độ dài UUID dùng để làm versioning, split để loại bỏ extension
                compare_file_name = ".".join(file.split(".")[:-1])  # loại bỏ extension string
                compare_file_name = compare_file_name[:-36]
                if file_name_prefix == compare_file_name:
                    api.delete_file(
                        path_in_repo=file,
                        repo_id=self.config["repoID"],
                        repo_type=self.config["repoType"],
                    )
        return
