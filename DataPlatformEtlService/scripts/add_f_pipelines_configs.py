import asyncio
from typing import Any, cast

from src.dtos import IAddFPipelineConfig
from src.routers.f_pipeline_configs import add_f_pipeline_config


async def main():
    async def add(payload: IAddFPipelineConfig):
        try:
            await add_f_pipeline_config(cast(Any, payload))
        except Exception as e:
            print(e)

    await add(
        payload={
            "nameLabel": "eduquiz",
            "nameValue": "eduquiz",
            "fExtConfigNameValue": "eduquiz",
            "fLoaderConfigNameValue": "eduquiz",
            "transferMode": "copyFile",
            "transformSql": None,
            "privateFieldPrefix": None,
            "includePrivateFields": None,
            "tagNameValue": None,
        }
    )


asyncio.run(main())
