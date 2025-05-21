from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError


class PostgresErrorUtils:
    @staticmethod
    def raise_if_not_unique_violation_error(e: IntegrityError):
        if isinstance(e.orig, UniqueViolation):
            return True
        raise e
