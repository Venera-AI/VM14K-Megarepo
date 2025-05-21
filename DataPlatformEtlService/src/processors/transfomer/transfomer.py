import pyarrow as pa
from typing import Any, List, cast
from src.processors.common import IProcessDataBlock, ProcessCommonConsts
from src.processors.exts import BaseExt
from src.processors.pipelines import IPipelineConfig
from src.utils.arrow_table import ArrowTableUtil


class Transformer:
    def __init__(self, pipeline_config: IPipelineConfig, ext: BaseExt):
        self.pipeline_config = pipeline_config
        self.ext = ext

    def transform(self, data_blocks: List[IProcessDataBlock]):
        new_data_blocks: List[IProcessDataBlock] = []
        include_private_fields = self.pipeline_config.get("includePrivateFields", None)
        private_field_prefix = self.pipeline_config.get("privateFieldPrefix", ProcessCommonConsts.PRIVATE_FIELD_PREFIX)
        transform_sql = self.pipeline_config.get("transformSql", None)
        tag_name_value = self.pipeline_config.get("tagNameValue", None)
        tag_name_field = f"{private_field_prefix}-tagNameValue"
        for data_block in data_blocks:
            # convert data file to arrow table
            new_data_block = cast(IProcessDataBlock, dict(data_block))  # Chuyển đổi thành dict
            if new_data_block["dataType"] == "file" and self.pipeline_config["transferMode"] == "arrowTable":
                files = new_data_block["data"]
                arrow_tables = []
                for file in files:
                    arrow_tables.append(self.ext.bytes_to_arrow_table(file))
                if len(arrow_tables) > 0:
                    concated_table = pa.concat_tables(arrow_tables)
                else:
                    concated_table = pa.table({})
                new_data_block["data"] = concated_table
                new_data_block["dataType"] = cast(Any, "arrowTable")
            data: pa.Table = new_data_block["data"]
            # add private fields
            if include_private_fields is not None:
                for private_field in include_private_fields:
                    full_private_field_name = f"{private_field_prefix}-{private_field}"
                    data.append_column(full_private_field_name, new_data_block["metadata"][private_field])
            # add tag name
            if tag_name_value is not None:
                data.append_column(tag_name_field, pa.array([tag_name_value] * len(data)))
            # transform sql if need
            if transform_sql is not None:
                data = ArrowTableUtil.transform_table_with_duckdb(
                    arrow_table=data,
                    sql_query=transform_sql,
                    table_name=ProcessCommonConsts.TRANSFOMER_DUCKDB_TABLE_NAME,
                )
                new_data_block["data"] = data
            new_data_blocks.append(new_data_block)
        return new_data_blocks
