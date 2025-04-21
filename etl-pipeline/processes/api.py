import os

from api.app import FlaskAPI
from logger import create_log
from managers import DBManager, ElasticsearchManager, RedisManager

logger = create_log(__name__)


class APIProcess:
    def __init__(self, *args):
        host = os.environ.get("POSTGRES_READ_HOST")
        self.db_manager = DBManager(host=host)
        self.elastic_search_manager = ElasticsearchManager()
        self.redis_manager = RedisManager()

    def runProcess(self):
        try:
            logger.info("Starting API...")

            db_engine = self.db_manager.generate_engine()
            redis_client = self.redis_manager.create_client()
            self.elastic_search_manager.create_elastic_connection()

            api = FlaskAPI(db_engine, redis_client)

            api.createErrorResponses()
            api.run()
        except Exception as e:
            logger.exception("Failed to start API")
            raise e
