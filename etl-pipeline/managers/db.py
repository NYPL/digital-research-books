import functools
import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

from model import Base
from logger import create_log
from utils.common import require_env

logger = create_log(__name__)

# Postgres SQLSTATE codes that indicate a transient, retryable failure.
#   40P01: deadlock_detected
#   40001: serialization_failure
# Any other OperationalError (connection drops, admin shutdown, etc.) is treated
# as terminal and propagated to the caller.
_RETRYABLE_PG_SQLSTATES = {"40P01", "40001"}


def _is_retryable_pg_error(exc: OperationalError) -> bool:
    orig = getattr(exc, "orig", None)
    pgcode = getattr(orig, "pgcode", None)
    return pgcode in _RETRYABLE_PG_SQLSTATES


def _retry_on_deadlock(max_attempts: int = 2):
    """Decorator for DBManager methods that perform a write + commit.

    On a retryable Postgres error (deadlock / serialization failure) the
    session is rolled back and the method is invoked again, up to
    ``max_attempts`` total attempts. On a non-retryable OperationalError, or
    after the final attempt, the session is rolled back and the exception is
    re-raised so callers see the failure.
    """

    def decorator(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return method(self, *args, **kwargs)
                except OperationalError as exc:
                    self.rollback_changes()
                    if not _is_retryable_pg_error(exc):
                        logger.error(
                            "Non-retryable database error in %s; re-raising",
                            method.__name__,
                            exc_info=exc,
                        )
                        raise
                    if attempt >= max_attempts:
                        logger.error(
                            "Retry exhausted for %s after "
                            "deadlock/serialization failure; re-raising",
                            method.__name__,
                            exc_info=exc,
                        )
                        raise
                    logger.warning(
                        "Deadlock/serialization failure in %s; "
                        "retrying (attempt %d/%d)",
                        method.__name__,
                        attempt,
                        max_attempts,
                        exc_info=exc,
                    )

        return wrapper

    return decorator


def get_database_url(user, pswd, host, port, db):
    return "postgresql://{}:{}@{}:{}/{}".format(user, pswd, host, port, db)


class DBManager:
    def __init__(self, user=None, pswd=None, host=None, port=None, db=None):
        super(DBManager, self).__init__()
        self.user = user or require_env("POSTGRES_USER")
        self.pswd = pswd or require_env("POSTGRES_PSWD")
        self.host = host or require_env("POSTGRES_HOST")
        self.port = port or require_env("POSTGRES_PORT")
        self.db = db or require_env("POSTGRES_NAME")

        self.engine = None
        self.session = None

    def __enter__(self):
        self.create_session()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close_connection()

    def generate_engine(self):
        try:
            self.engine = create_engine(
                get_database_url(self.user, self.pswd, self.host, self.port, self.db)
            )

            return self.engine
        except Exception as e:
            raise e

    def initialize_database(self):
        if not inspect(self.engine).has_table("works"):
            Base.metadata.create_all(self.engine)

    def create_session(self, autoflush=False):
        if not self.engine:
            self.generate_engine()
        self.session = sessionmaker(bind=self.engine, autoflush=autoflush)()

    def start_session(self):
        self.session.begin_nested()

    @_retry_on_deadlock()
    def commit_changes(self):
        """Commit the current transaction.

        Retried once on deadlock / serialization failure; otherwise the
        OperationalError is rolled back and propagated.
        """
        self.session.commit()

    def rollback_changes(self):
        self.session.rollback()

    def close_connection(self):
        if self.session is not None:
            self.session.close()
            self.session = None
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None

    @_retry_on_deadlock()
    def bulk_save_objects(self, objects, only_changed=True):
        self.session.bulk_save_objects(objects, update_changed_only=only_changed)
        self.session.commit()

    def windowed_query(self, stmt, id_column, windowsize):
        """
        Yields all records from stmt, fetching `windowsize` records at a time into memory.
        `column` must contain strictly unique values (non-null)
        Safe to call session.commit() while iterating.
        see: https://github.com/sqlalchemy/sqlalchemy/wiki/RangeQuery-and-WindowedRangeQuery
        """
        stmt = stmt.add_columns(id_column).order_by(id_column)
        last_id = None

        while True:
            subq = stmt

            if last_id is not None:
                subq = subq.filter(id_column > last_id)

            result = self.session.execute(subq.limit(windowsize))
            chunk = result.all()

            if not chunk:
                break

            last_id = chunk[-1][-1]

            for row in chunk:
                yield row[0]

    @_retry_on_deadlock()
    def delete_records_by_query(self, query):
        """Execute a bulk DELETE for the given query and commit.

        Previously this method neither committed nor surfaced errors, so
        callers could believe a delete had succeeded when no rows had been
        removed. The commit is now part of the operation, and errors are
        rolled back and re-raised (with deadlock retry handled by the
        decorator).
        """
        query.delete()
        self.session.commit()
