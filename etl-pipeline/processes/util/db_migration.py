from alembic import config
from logger import create_log

logger = create_log(__name__)


class MigrationProcess:
    def __init__(self, *args):
        self.options = args[6]

    def runProcess(self):
        try:
            logger.info("Running database migration")

            alembic_args = ["--raiseerr"] + self.options

            config.main(argv=alembic_args)
        except Exception as e:
            logger.exception("Failed to run database migration")
            raise e
