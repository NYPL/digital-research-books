from api.app import API
from logger import create_log
from managers import ElasticsearchManager, RedisManager

logger = create_log(__name__)


class APIProcess:
    def __init__(self, *args):
        self.elastic_search_manager = ElasticsearchManager()
        self.redis_manager = RedisManager()

    def run(self):
        try:
            logger.info("Starting API...")

            redis_client = self.redis_manager.create_client()
            self.elastic_search_manager.create_elastic_connection()

            api = API(redis_client)

            api.create_error_responses()
            api.run()
        except Exception as e:
            logger.exception("Failed to start API")
            raise e
