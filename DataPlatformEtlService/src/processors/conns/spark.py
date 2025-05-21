from pyspark import SparkConf
from pyspark.sql import SparkSession

from typing import Dict


class SparkConnPool:
    def __init__(self, inputs: Dict):
        self.dns = inputs["dns"]
        self.app_name = inputs["appName"]
        self.max_conn = inputs.get("maxConn", 1)
        self.min_conn = inputs.get("minConn", 1)
        self.config = inputs.get("config", {})
        self.spark_conf = SparkConf()
        for key, value in self.config.items():
            self.spark_conf.set(key, value)

    def __get_conn__(self):
        spark = SparkSession.builder
        if self.dns[0:5] == "sc://":
            spark = spark.remote(self.dns)  # pyright: ignore [reportAttributeAccessIssue]
        else:
            spark = spark.master(self.dns)  # pyright: ignore [reportAttributeAccessIssue]
        spark = spark.config(conf=self.spark_conf).appName(self.app_name)
        return spark.getOrCreate()

    def get(self):
        return self.__get_conn__()

    @classmethod
    def put(cls, session):
        session.stop()
        pass

    @classmethod
    def commit(cls, session):
        # session.commit()
        pass

    @classmethod
    def rollback(cls, session):
        # session.rollback()
        pass
