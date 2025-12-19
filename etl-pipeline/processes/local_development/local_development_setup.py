from elasticsearch7.exceptions import ConnectionError
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sqlalchemy as sa
from sqlalchemy.exc import ProgrammingError
from time import sleep

from managers import DBManager, ElasticsearchManager
from logger import create_log

logger = create_log(__name__)


class LocalDevelopmentSetupProcess:
    def __init__(self, *args):
        self.elastic_search_manager = ElasticsearchManager()
        self.db_manager = DBManager()

    def run(self):
        try:
            self.initialize_db()

            self.db_manager.create_session()
            self.db_manager.initialize_database()

            self.elastic_search_manager.create_elastic_connection()
            self.wait_for_elastic_search()
            self.elastic_search_manager.create_elastic_search_index()

            logger.info("Completed local development setup")
        except Exception:
            logger.exception("Failed to run development setup process")

    def initialize_db(self):
        admin_db_manager = DBManager(
            user=os.environ["ADMIN_USER"],
            pswd=os.environ["ADMIN_PSWD"],
            host=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            db="postgres",
        )

        admin_db_manager.generate_engine()

        with admin_db_manager.engine.connect() as admin_db_connection:
            admin_db_connection.connection.set_isolation_level(
                ISOLATION_LEVEL_AUTOCOMMIT
            )

            self.create_database(admin_db_connection)
            self.create_database_user(admin_db_connection)

        admin_db_manager.engine.dispose()

    def create_database(self, db_connection):
        try:
            db_connection.execute(
                sa.text(f"CREATE DATABASE {os.environ['POSTGRES_NAME']}"),
            )
        except ProgrammingError:
            pass
        except Exception as e:
            logger.exception("Failed to create database")
            raise e

    def create_database_user(self, db_connection):
        postgres_user = os.environ.get("POSTGRES_USER")
        postgres_pswd = os.environ.get("POSTGRES_PSWD")
        database = os.environ.get("POSTGRES_NAME")

        try:
            db_connection.execute(
                sa.text(f"CREATE USER {postgres_user} WITH PASSWORD '{postgres_pswd}'")
            )
        except ProgrammingError:
            pass
        except Exception as e:
            logger.exception("Failed to create database user")
            raise e

        db_connection.execute(
            sa.text(f"GRANT ALL ON DATABASE {database} TO {postgres_user}")
        )
        db_connection.execute(
            sa.text(f"ALTER DATABASE {database} OWNER TO {postgres_user}")
        )

    def wait_for_elastic_search(self):
        increment = 5
        max_time = 60

        for _ in range(0, max_time, increment):
            try:
                self.elastic_search_manager.es.info()
                break
            except ConnectionError:
                pass
            except Exception as e:
                logger.exception("Failed to wait for elastic search")
                raise e

            sleep(increment)
