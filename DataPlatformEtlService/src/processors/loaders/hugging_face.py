from huggingface_hub import HfApi
import jinja2
from src.db.sessions.base import BaseSession
from src.processors.common import IProcessDataBlock
from src.processors.conns import HuggingFaceConnPool
from src.processors.loader_configs import IHuggingFaceLoaderConfig, HuggingfaceLoaderConfigDTO
from src.processors.loaders import BaseLoader


class HuggingFaceLoader(BaseLoader[IHuggingFaceLoaderConfig]):
    def __init__(
        self,
        config,
        pipeline_name,
        execution_time,
    ):
        super().__init__(
            config=config,
            pipeline_name=pipeline_name,
            execution_time=execution_time,
        )
        self.config = self.root_config["config"]
        self.connector = BaseSession[HfApi](
            inputs={"credentials": self.config["connConfig"]["config"]}, pool_class=HuggingFaceConnPool
        ).generate_session_scope_func()
        self.batch_size = 1000

    @classmethod
    def is_compatible_with_transfer_copy_file(cls):
        return True

    @classmethod
    def validate_root_config(cls, config):
        return HuggingfaceLoaderConfigDTO.validate_python(config)

    async def __put_data__(self, file_name, file):
        with self.connector() as api:
            api.upload_file(
                path_or_fileobj=file,
                path_in_repo=file_name,
                repo_id=self.config["repoID"],
                repo_type=self.config["repoType"],
            )

    async def push(self, data_block: IProcessDataBlock):
        # if data_block["isEnd"] is True:
        #     return
        # if data_block["dataType"] == "files":
        #     pass
        # else:
        #     raise ValueError("only support transfer files")
        folder = self.config["folder"]
        folder = jinja2.Template(folder).render(execution_time=self.execution_time)
        for data_i in range(len(data_block["data"])):
            data = data_block["data"][data_i]
            file_info = data_block["fileInfo"][buffer_i]
            file_path = f"{folder}/{file_info['path']}"
            await self.__put_data__(file_name=file_path, file=buffer)
        return
