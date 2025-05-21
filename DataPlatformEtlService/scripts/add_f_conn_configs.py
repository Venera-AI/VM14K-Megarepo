import asyncio
import yaml

from src.dtos import IAddFConnConfig
from src.processors.conn_configs import IAwsStorageConnConfig, IHuggingFaceConnConfig
from src.routers.f_conn_configs import add_f_connector_config


async def main():
    async def add(payload: IAddFConnConfig):
        try:
            await add_f_connector_config(payload)
        except Exception as e:
            print(e)

    with open("./scripts/connector_configs.yaml", "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    medical_lake_config: IAwsStorageConnConfig = {
        "type": "awsStorage",
        "config": {
            "accessKeyId": config["medicalLake"]["accessKeyId"],
            "secretAccessKey": config["medicalLake"]["secretAccessKey"],
        },
    }
    await add(
        payload={
            "nameLabel": "Medical lake",
            "nameValue": "medicalLake",
            "type": medical_lake_config["type"],
            "config": medical_lake_config["config"],
        }
    )

    medical_warehouse_config: IHuggingFaceConnConfig = {
        "type": "huggingFace",
        "config": {
            "accessToken": config["medicalWarehouse"]["accessToken"],
        },
    }
    await add(
        payload={
            "nameLabel": "Medical Warehouse",
            "nameValue": "medicalWarehouse",
            "type": medical_warehouse_config["type"],
            "config": medical_warehouse_config["config"],
        }
    )


asyncio.run(main())
