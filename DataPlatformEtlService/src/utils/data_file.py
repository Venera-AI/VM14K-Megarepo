import pyarrow as pa
import pyarrow.parquet as pq
import bz2
from typing import Any, cast


class DataFileUtil:
    @staticmethod
    def uncompress_file(file: bytes, compress_type: str):
        if compress_type == "bzip2":
            return bz2.decompress(cast(Any, file))
        else:
            raise ValueError(f"not support compress type {compress_type}")

    @staticmethod
    def bytes_to_arrow_table(data: bytes, file_type: str) -> pa.Table:
        if file_type == "parquet":
            return pq.read_table(pa.BufferReader(data))
        else:
            raise ValueError(f"file type {file_type} not supported")
