import glob
import posixpath
import re
import pyarrow as pa
import jinja2
from typing import Dict, List
import boto3
from src.db.sessions import BaseSession
from src.processors.common import IProcessDataBlock
from src.processors.conns import AwsStorageConnPool
from src.processors.exts import BaseExt
from src.processors.ext_configs import IAwsStorageExtConfig, AwsStorageExtConfigDTO
from src.utils.aws import AwsS3Util
from src.utils.data_file import DataFileUtil
from src.utils.logger import LOGGER


class AwsStorageExt(BaseExt[IAwsStorageExtConfig]):
    def __init__(
        self,
        config,
        execution_time,
    ):
        super().__init__(
            config=config,
            execution_time=execution_time,
        )
        self.config = self.root_config["config"]
        self.aws_connector = BaseSession[boto3.Session](
            inputs={"credentials": self.config["connConfig"]["config"]}, pool_class=AwsStorageConnPool
        ).generate_session_scope_func()
        self.aws_continuation_token = None

    @classmethod
    def is_compatible_with_transfer_copy_file(cls):
        return True

    @classmethod
    def validate_root_config(cls, config):
        return AwsStorageExtConfigDTO.validate_python(config)

    async def extract(self, init_metadata_values: Dict) -> List[IProcessDataBlock]:
        prefix = self.config.get("prefix", "/")
        if prefix is not None:
            prefix = jinja2.Template(prefix).render(execution_time=self.execution_time)
        path_glob_filter = self.config.get("pathGlobFilter", None)
        if path_glob_filter is not None:
            path_glob_filter = jinja2.Template(path_glob_filter).render(execution_time=self.execution_time)
        remove_prefix_path = self.config.get("removePrefixPath", None)
        if remove_prefix_path is not None:
            remove_prefix_path = jinja2.Template(remove_prefix_path).render(execution_time=self.execution_time)
        LOGGER.info(f"[PIPELINE][EXT][AWS_STORAGE] start extract {prefix} {path_glob_filter}")
        with self.aws_connector() as session:
            client = session.client("s3")
            blobs = await AwsS3Util.list_objects_v2(
                client=client,
                Bucket=self.config["bucketName"],
                Prefix=prefix,
                ContinuationToken=self.aws_continuation_token,
            )
            self.aws_continuation_token = blobs.get("NextContinuationToken", None)
            self.aws_continuation_token = (
                None
                if self.aws_continuation_token is None or self.aws_continuation_token.strip() == ""
                else self.aws_continuation_token
            )
            data_blocks: List[IProcessDataBlock] = []
            LOGGER.info(f"[PIPELINE][EXT][AWS_STORAGE] found {blobs['KeyCount']} files")
            if blobs["KeyCount"] == 0:
                data_block: IProcessDataBlock = {
                    "isEnd": True,
                    "data": [],
                    "dataType": "file",
                }
                data_blocks.append(data_block)
                return data_blocks
            content_blobs = blobs["Contents"]
            regex = None
            if path_glob_filter is not None:
                full_path_pattern = posixpath.join(prefix, path_glob_filter)
                regex = re.compile(
                    glob.translate(full_path_pattern, recursive=self.config["recursive"], include_hidden=True)
                )
            for content_blob_i in range(len(content_blobs)):
                content_blob = content_blobs[content_blob_i]
                path = content_blob["Key"]
                if regex is not None and not regex.match(path):
                    continue
                LOGGER.info(f"[PIPELINE][EXT][AWS_STORAGE] download {path}")
                data_buffer = await AwsS3Util.download_object(
                    client=client,
                    bucket_name=self.config["bucketName"],
                    object_key=path,
                )
                removed_path = path.replace(f"{remove_prefix_path}/", "") if remove_prefix_path else path
                tag_name_value = init_metadata_values.get("tagNameValue", None)
                data_block: IProcessDataBlock = {
                    "data": [data_buffer.read()],
                    "isEnd": False,
                    "dataType": "file",
                    "metadata": {
                        "path": pa.table({"path": [removed_path]}),
                        "prefix": pa.table({"prefix": [prefix]}),
                        "extNameValue": pa.table({"extNameValue": [init_metadata_values["extNameValue"]]}),
                        "pipelineNameValue": pa.table(
                            {"pipelineNameValue": [init_metadata_values["pipelineNameValue"]]}
                        ),
                        "tagNameValue": (
                            pa.table({"tagNameValue": [init_metadata_values["tagNameValue"]]})
                            if tag_name_value is not None
                            else None
                        ),
                        "incrementalValue": pa.table({"incrementalValue": [removed_path]}),
                    },
                }
                data_blocks.append(data_block)
            if self.aws_continuation_token is None:
                data_blocks.append(
                    {
                        "isEnd": True,
                        "data": [],
                        "dataType": "file",
                    }
                )
        return data_blocks

    def bytes_to_arrow_table(self, data: bytes):
        compress_type = self.config.get("compressType", None)
        if compress_type is not None:
            data = DataFileUtil.uncompress_file(file=data, compress_type=compress_type)
        return DataFileUtil.bytes_to_arrow_table(data=data, file_type=self.config["fileType"])
