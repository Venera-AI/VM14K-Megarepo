from .postgres import PostgresConnectorPool
import contextvars

CONTEXTVAR = contextvars.ContextVar[str]("var", default="")
