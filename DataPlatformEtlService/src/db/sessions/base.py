from contextlib import _GeneratorContextManager, contextmanager
from typing import Any, Callable, Dict, Generic, TypeVar
import uuid

from src.custom_types.common import BaseDict
from src.db.connectors import CONTEXTVAR
from src.utils.logger import LOGGER

T = TypeVar("T")


class BaseSession(Generic[T]):

    def __init__(self, inputs: BaseDict, pool_class):
        self.inputs = inputs
        self.pool_class = pool_class
        self.pool = self.pool_class(inputs=self.inputs)
        self.sessions: Dict[Any, T] = {}

    def set_session(self, session):
        context_id = CONTEXTVAR.get()
        if session is None:
            del self.sessions[context_id]
            return
        if context_id is None:
            context_id = uuid.uuid4()
        CONTEXTVAR.set(context_id)  # type: ignore
        self.sessions[context_id] = session

    def get_session(self) -> T | None:
        context_id = CONTEXTVAR.get()
        return self.sessions.get(context_id, None)

    def generate_session_scope_func(self) -> Callable[..., _GeneratorContextManager[T]]:
        @contextmanager
        def session_scope(new=False):
            """
            Provide a transactional scope around a series of operations.
            Shouldn't keep session alive too long, it will block a connection of pool connections.
            """
            if not new:
                session: T
                reuse_session = self.get_session()
                if reuse_session is None:
                    session = self.pool.get()
                    self.set_session(session=session)
                else:
                    session = reuse_session
                try:
                    yield session
                except Exception as exception:
                    # LOGGER.error(exception, exc_info=True)
                    if reuse_session is None:
                        self.pool.rollback(session)
                    raise exception
                else:
                    if reuse_session is None:
                        self.pool.commit(session)
                finally:
                    if reuse_session is None:
                        self.pool.put(session)
                        self.set_session(session=None)
            else:
                session = self.pool.get()
                try:
                    yield session
                except Exception as exception:
                    LOGGER.error(exception, exc_info=True)
                    self.pool.rollback(session)
                    raise exception
                else:
                    self.pool.commit(session)
                finally:
                    self.pool.put(session)

        return session_scope


# import os
# import threading
# from contextlib import asynccontextmanager
# from typing import AsyncContextManager
# import uuid
#
# from sqlalchemy.ext.asyncio import AsyncSession
#
# from src.db.connectors import CONTEXTVAR, SQLServerConnectorPool
# from src.utils.logger import LOGGER
#
# PREFIX = "BACKEND"
# SESSIONS = {}
# DNS = os.environ.get(f"{PREFIX}_DNS", None)
# MIN_CONN = 4
# MAX_CONN = 8
#
# POOL = SQLServerConnectorPool(dns=DNS, max_conn=MAX_CONN, min_conn=MIN_CONN)
#
#
# async def set_session(session):
#     global SESSIONS
#     context_id = CONTEXTVAR.get()
#     if session is None:
#         del SESSIONS[context_id]
#         return
#     if context_id is None:
#         context_id = uuid.uuid4()
#     CONTEXTVAR.set(context_id)
#     SESSIONS[context_id] = session
#
#
# async def get_session():
#     global SESSIONS
#     context_id = CONTEXTVAR.get()
#     return SESSIONS.get(context_id, None)
#
#
# @asynccontextmanager
# async def backend_session_scope(new=False) -> AsyncContextManager[AsyncSession]:
#     """
#     Provide a transactional scope around a series of operations.
#     Shouldn't keep session alive too long, it will block a connection of pool connections.
#     """
#     if not new:
#         session: AsyncSession
#         reuse_session = await get_session()
#         if reuse_session is None:
#             session = await POOL.get()
#             await set_session(session=session)
#         else:
#             session = reuse_session
#         try:
#             async with session as async_session:
#                 yield async_session
#                 if reuse_session is None:
#                     await async_session.commit()
#         except Exception as exception:
#             LOGGER.error(exception, exc_info=True)
#             if reuse_session is None:
#                 async with session as async_session:
#                     await async_session.rollback()
#             raise exception
#         finally:
#             if reuse_session is None:
#                 await POOL.put(session)
#                 await set_session(session=None)
#     else:
#         session = await POOL.get()
#         try:
#             yield session
#             async with session as async_session:
#                 await async_session.commit()
#         except Exception as exception:
#             LOGGER.error(exception, exc_info=True)
#             async with session as async_session:
#                 await async_session.rollback()
#             raise exception
#         finally:
#             await POOL.put(session)
