import jinja2
import posixpath
from datetime import datetime
from typing import Any
from pyspark.sql.functions import input_file_name

from src.db.sessions import BaseSession
from src.processors.conns import SparkConnPool

import logging

from src.processors.spark_transfomer import ISparkTransformerConfig
from src.utils.logger import LOGGER

logger = logging.getLogger("py4j")
logger.setLevel(logging.ERROR)


# def path_exists(spark, path):
#     # spark is a SparkSession
#     sc = spark.sparkContext
#     hadoop_conf = sc._jsc.hadoopConfiguration()
#     glob_path = sc._jvm.org.apache.hadoop.fs.Path(path)
#     fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(glob_path.toUri(), hadoop_conf)
#     files = fs.globStatus(glob_path)
#     if files and len(files) > 0:
#         return True
#     return False


class SparkTransformer:
    BASE_SPARK_CONFIG = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    }

    def __init__(self, config: ISparkTransformerConfig, spark_dns, spark_app_name):
        self.config = config
        self.spark_session_scope = BaseSession[Any](
            inputs={
                "dns": spark_dns,
                "appName": str(spark_app_name),
                "config": {
                    **self.__class__.BASE_SPARK_CONFIG,
                    "spark.hadoop.fs.s3a.access.key": self.config["storageConnConfig"]["config"]["accessKeyId"],
                    "spark.hadoop.fs.s3a.secret.key": self.config["storageConnConfig"]["config"]["secretAccessKey"],
                },
            },
            pool_class=SparkConnPool,
        ).generate_session_scope_func()
        self.runtime_config = {
            "ignorePathNotFound": True,
        }

    def generate_query(self, execution_time: datetime | None | str):
        if execution_time is None:
            execution_time = "*"
        src_folder = jinja2.Template(self.config["srcFolder"]).render(execution_time=execution_time)
        with self.spark_session_scope() as spark:
            for view_config in self.config["srcViewConfig"]:
                path = posixpath.join(src_folder, view_config.get("path", ""))
                path_glob_filter = view_config.get("pathGlobFilter", None)
                if path_glob_filter is not None:
                    path_glob_filter = jinja2.Template(path_glob_filter).render(execution_time=execution_time)
                compress_type = view_config.get("compressType", None)
                view_df = spark.read
                view_df = view_df.option("recursiveFileLookup", view_config["recursiveFileLookup"])
                if view_config["fileType"] == "json":
                    view_df = view_df.option("format", "json")
                else:
                    raise ValueError(f"fileType {view_config['fileType']} not supported")
                if compress_type is not None:
                    view_df = view_df.option("compression", compress_type)
                # if self.runtime_config["ignorePathNotFound"] is True and not path_exists(spark, full_filename):
                #     return
                try:
                    LOGGER.info(f"[PIPELINE][EXT][SPARK_TRANSFORMER] read {path} with {path_glob_filter} glob filter")
                    if path_glob_filter is not None:
                        view_df = view_df.json(path, pathGlobFilter=path_glob_filter)
                    else:
                        view_df = view_df.json(path)
                except Exception as e:
                    if "[PATH_NOT_FOUND]" in str(e):
                        return
                view_df = view_df.withColumn(view_config.get("filename_field", "__filename__"), input_file_name())
                view_df.createOrReplaceTempView(view_config["alias"])
            df = spark.sql(self.config["transformSql"])
            return df

    def transform(self, execution_time: datetime | None | str):
        if execution_time is None:
            execution_time = "*"
        with self.spark_session_scope():
            df = self.generate_query(execution_time)
            if df is None:
                return
            dst_folder = jinja2.Template(self.config["dstFolder"]).render(execution_time=execution_time)
            df.write.mode("overwrite").parquet(dst_folder)
