from sqlalchemy import text

from logger import create_log
from managers import DBManager

logger = create_log(__name__)


class DatabaseMaintenanceProcess:
    VACUUMING_TABLES = [
        "records",
        "items",
        "editions",
        "works",
        "links",
        "rights",
        "identifiers",
    ]

    def __init__(self, *args):
        self.db_manager = DBManager()

    def runProcess(self):
        try:
            self.db_manager.create_session()

            self.vacuum_tables()

            logger.info("Database maintenance complete")
        except Exception as e:
            logger.exception("Failed to run database maintenance")
            raise e
        finally:
            self.db_manager.close_connection()

    def vacuum_tables(self):
        logger.info(f"Vacuuming tables: {self.VACUUMING_TABLES}")

        with (
            self.db_manager.engine.connect() as db_connnection,
            db_connnection.execution_options(isolation_level="AUTOCOMMIT"),
        ):
            for table_name in self.VACUUMING_TABLES:
                logger.info(f"Vacuuming {table_name} table")
                db_connnection.execute(text(f"VACUUM ANALYZE {table_name};"))
