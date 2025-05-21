from typing import Dict
import pyodbc

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool

from src.utils.logger import LOGGER


class SqlServerConnPool:
    def __init__(self, inputs: Dict):
        self.dns = inputs["dns"]
        self.max_conn = inputs.get("max_conn", 1)
        self.min_conn = inputs.get("min_conn", 1)
        self.engine = create_engine(
            # DNS,
            "mssql+pyodbc://",
            poolclass=QueuePool,
            pool_pre_ping=True,
            pool_size=self.max_conn - self.min_conn,
            max_overflow=self.min_conn,
            pool_timeout=60 * 60,
            creator=self.__get_conn__,
        )
        try:
            session = self.get()
            self.put(session)
            LOGGER.info("Successfully create connection...")
        except Exception as e:
            raise Exception(f"Could not create connection to {self.dns}", e)

    def __get_conn__(self):
        c = pyodbc.connect(self.dns)
        return c

    def get(self) -> Session:
        return Session(bind=self.engine.connect())

    @classmethod
    def put(cls, session):
        session.close()

    @classmethod
    def commit(cls, session):
        session.commit()

    @classmethod
    def rollback(cls, session):
        session.rollback()
