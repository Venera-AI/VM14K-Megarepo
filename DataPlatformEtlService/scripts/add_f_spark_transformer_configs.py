import asyncio
from typing import Any, cast

from src.dtos import IAddFSparkTransformerConfig
from src.routers.f_spark_transformer_configs import add_f_spark_transformer_config


async def main():
    async def add(payload: IAddFSparkTransformerConfig):
        try:
            await add_f_spark_transformer_config(cast(Any, payload))
        except Exception as e:
            print(e)

    await add(
        payload={
            "nameLabel": "eduquiz",
            "nameValue": "eduquiz",
            "storageFConnConfigNameValue": "medicalLake",
            "srcFolder": (
                "s3a://medical-datalake-dev/crawler/eduquiz"
                "{{'/' + execution_time.strftime('%Y-%m-%d') if execution_time != '*' else ''}}"
            ),
            "dstFolder": "s3a://medical-datalake-dev/bronze/eduquiz",
            "srcViewConfig": [
                {
                    "alias": "eduquiz",
                    "pathGlobFilter": "*.data.json.bz2",
                    "fileType": "json",
                    "compressType": "bzip2",
                    "filenameField": "__filename__",
                    "recursiveFileLookup": True,
                }
            ],
            "transformSql": "SELECT * FROM eduquiz",
        }
    )


asyncio.run(main())
