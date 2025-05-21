from typing import Dict
import psycopg2

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool

from src.utils.logger import LOGGER

# pyodbc.pooling = False


class PostgresConnectorPool:
    def __init__(self, inputs: Dict):
        self.dns = inputs["dns"]
        self.max_conn = inputs["max_conn"]
        self.min_conn = inputs["min_conn"]
        self.engine = create_engine(
            # DNS,
            "postgresql+psycopg2://",
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
        c = psycopg2.connect(self.dns)
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
