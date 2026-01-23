import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

from model import Base
from logger import create_log
from utils.common import require_env

logger = create_log(__name__)


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
        self.create_session(autoflush=True)
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

    def commit_changes(self, retry=False):
        """
        retry=True indicates the function is being called as a retry.
        When retry=True, the function will NOT retry itself.
        When retry=False, the function will retry itself once.
        """
        try:
            self.session.commit()
        except OperationalError as oprErr:
            logger.error("Deadlock in database layer, retry batch")
            logger.debug(oprErr)

            self.rollback_changes()

            if retry is False:
                self.commit_changes(retry=True)
            else:
                logger.warning("Already retried batch, dropping")

    def rollback_changes(self):
        self.session.rollback()

    def close_connection(self):
        self.session.close()
        self.engine.dispose()

    def bulk_save_objects(self, objects, only_changed=True, retry=False):
        try:
            self.session.bulk_save_objects(objects, update_changed_only=only_changed)
            self.session.commit()
            self.session.flush()
        except OperationalError as oprErr:
            logger.error("Deadlock in database layer, retry batch")
            logger.debug(oprErr)

            self.rollback_changes()

            if retry is False:
                self.bulk_save_objects(objects, only_changed=only_changed, retry=True)
            else:
                logger.warning("Already retried batch, dropping")

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

    def delete_records_by_query(self, query):
        try:
            query.delete()
        except OperationalError as oprErr:
            logger.error("Deadlock in database layer, retry batch")
            logger.debug(oprErr)
