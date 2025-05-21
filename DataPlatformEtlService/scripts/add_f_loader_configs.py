import asyncio
from typing import Any, cast

from src.dtos import IAddFLoaderConfig
from src.routers.f_loader_configs import add_f_loader_config


async def main():
    async def add(payload: IAddFLoaderConfig):
        try:
            await add_f_loader_config(cast(Any, payload))
        except Exception as e:
            print(e)

    await add(
        payload={
            "nameLabel": "eduquiz",
            "nameValue": "eduquiz",
            "type": "huggingFace",
            "config": {
                "repoID": "venera-ai/medical-warehouse-dev",
                "repoType": "dataset",
                "folder": "eduquiz",
            },
            "connDeps": {
                "connConfig": "medicalWarehouse",
            },
        }
    )


asyncio.run(main())
