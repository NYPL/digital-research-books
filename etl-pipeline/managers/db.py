import os
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

from model import Base
from logger import create_log

logger = create_log(__name__)


class DBManager:
    def __init__(self, user=None, pswd=None, host=None, port=None, db=None):
        super(DBManager, self).__init__()
        self.user = user or DBManager.decrypt_env_var("POSTGRES_USER")
        self.pswd = pswd or DBManager.decrypt_env_var("POSTGRES_PSWD")
        self.host = host or DBManager.decrypt_env_var("POSTGRES_HOST")
        self.port = port or DBManager.decrypt_env_var("POSTGRES_PORT")
        self.db = db or DBManager.decrypt_env_var("POSTGRES_NAME")

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
                "postgresql://{}:{}@{}:{}/{}".format(
                    self.user, self.pswd, self.host, self.port, self.db
                )
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

    def windowed_query(self, table, query, window_size=100, ingest_limit=None):
        single_entity = query.is_single_entity
        query = query.add_column(table.date_modified).order_by(table.date_modified)
        query = query.add_column(table.id).order_by(table.id)

        last_id = None
        total_fetched = 0

        while True:
            sub_query = query

            if last_id is not None:
                sub_query = sub_query.filter(table.id > last_id)

            query_chunk = sub_query.limit(window_size).all()
            total_fetched += window_size

            if not query_chunk or (ingest_limit and total_fetched > ingest_limit):
                break

            last_id = query_chunk[-1][-1]

            for row in query_chunk:
                yield row[0] if single_entity else row[0:-2]

    def delete_records_by_query(self, query):
        try:
            query.delete()
        except OperationalError as oprErr:
            logger.error("Deadlock in database layer, retry batch")
            logger.debug(oprErr)

    @staticmethod
    def decrypt_env_var(envVar):
        encrypted = os.environ.get(envVar, None)
        # TODO Implement Kubernetes encryption
        return encrypted
