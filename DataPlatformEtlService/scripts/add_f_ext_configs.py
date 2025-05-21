import asyncio
from typing import Any, cast

from src.dtos.add_f_ext_configs import IAddFExtConfig
from src.routers.f_ext_configs import add_f_ext_config


async def main():
    async def add(payload: IAddFExtConfig):
        try:
            await add_f_ext_config(cast(Any, payload))
        except Exception as e:
            print(e)

    await add(
        payload={
            "nameLabel": "eduquiz",
            "nameValue": "eduquiz",
            "type": "awsStorage",
            "config": {
                "bucketName": "medical-datalake-dev",
                "prefix": (
                    "bronze/eduquiz{{'/' + execution_time.strftime('%Y-%m-%d') if execution_time != '*' else ''}}"
                ),
                "pathGlobFilter": "**/*.parquet",
                "recursive": True,
                "removePrefixPath": "bronze/eduquiz",
                "fileType": "parquet",
                "compressType": None,
            },
            "connDeps": {
                "connConfig": "medicalLake",
            },
        }
    )


asyncio.run(main())
