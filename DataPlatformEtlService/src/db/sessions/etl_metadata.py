from sqlalchemy.orm import Session

from src.consts import EtlMetadataConsts
from src.db.connectors import PostgresConnectorPool
from src.db.sessions import BaseSession


ETL_METADATA_SESSION_SCOPE = BaseSession[Session](
    inputs=EtlMetadataConsts.CREDENTIALS, pool_class=PostgresConnectorPool
).generate_session_scope_func()
