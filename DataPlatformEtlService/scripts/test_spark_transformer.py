import asyncio
from datetime import datetime


from src.consts.etl_metadata import EtlMetadataConsts
from src.processors.spark_transfomer.transfomer import SparkTransformer
from src.services import F_SPARK_TRANSFORMATION_CONFIG_SERVICE


async def main():
    _, config = await F_SPARK_TRANSFORMATION_CONFIG_SERVICE.get_config_by_name_value("eduquiz")
    if config is None:
        raise Exception("config not found")
    spark_transformer = SparkTransformer(config, EtlMetadataConsts.SPARK_DNS, 1)
    context = spark_transformer.spark_session_scope()
    context.__enter__()
    df = spark_transformer.generate_query(datetime.strptime("2025-05-07", "%Y-%m-%d"))
    print(df)


asyncio.run(main())
