from datetime import datetime, timedelta, timezone
from typing import Union
import os
from redis import Redis

from logger import create_log

logger = create_log(__name__)

ONE_WEEK = 60 * 60 * 24 * 7


class RedisManager:
    def __init__(self, host=None, port=None):
        super(RedisManager, self).__init__()
        self.host = host or os.environ.get("REDIS_HOST", None)
        self.port = port or os.environ.get("REDIS_PORT", None)
        self.environment = os.environ.get("ENVIRONMENT", "test")

        self.present_time = datetime.now(timezone.utc).replace(tzinfo=None)
        self.one_day_ago = self.present_time - timedelta(days=1)

        self.oclc_limit = int(os.environ.get("OCLC_QUERY_LIMIT", 400000))

    def create_client(self) -> Redis:
        self.client = Redis(host=self.host, port=self.port, socket_timeout=5)
        return self.client

    def clear_cache(self):
        self.client.flushall()

    def check_or_set_key(
        self,
        service: str,
        identifier: Union[int, str],
        identifier_type: str,
        expiration_time: int = ONE_WEEK,
    ) -> bool:
        query_time = self.client.get(
            f"{self.environment}/{service}/{identifier}/{identifier_type}"
        )

        if query_time is not None and datetime.strptime(query_time.decode("utf-8"), "%Y-%m-%dT%H:%M:%S") >= self.one_day_ago:
            logger.debug(f"Identifier {identifier} recently queried")
            return True

        self.set_key(service, identifier, identifier_type, expiration_time=expiration_time)
        return False

    def multi_check_or_set_key(
        self,
        service: str,
        identifiers: list[Union[int, str]],
        identifier_type: str,
        expiration_time: int = ONE_WEEK,
    ) -> list[tuple[Union[str, int], bool]]:
        keys_to_check = [
            f"{self.environment}/{service}/{identifier}/{identifier_type}"
            for identifier in identifiers
        ]

        identifier_query_times = self.client.mget(keys_to_check)

        output = []
        for i, query_time in enumerate(identifier_query_times):
            update_required = True
            if query_time is not None and datetime.strptime(query_time.decode("utf-8"), "%Y-%m-%dT%H:%M:%S") >= self.one_day_ago:
                logger.debug(f"Identifier {identifiers[i]} recently queried")
                update_required = False

            output.append((identifiers[i], update_required))

        keys_to_set = [key[0] for key in output if key[1] is True]
        self.multi_set_key(service, keys_to_set, identifier_type, expiration_time=expiration_time)

        return output

    def set_key(
        self,
        service: str,
        identifier: Union[int, str],
        identifier_type: str,
        expiration_time: int = ONE_WEEK,
    ):
        self.client.set(
            f"{self.environment}/{service}/{identifier}/{identifier_type}",
            self.present_time.strftime("%Y-%m-%dT%H:%M:%S"),
            ex=expiration_time,
        )

    def any_locked(self, keys: list) -> bool:
        keys = [f'redlock:{key}' for key in keys]
        
        return any(locks is not None for locks in self.client.mget(keys))

    def multi_set_key(
        self,
        service: str,
        identifiers: list[Union[int, str]],
        identifier_type: str,
        expiration_time: int = ONE_WEEK,
    ):
        pipe = self.client.pipeline()

        for identifier in identifiers:
            pipe.set(
                f"{self.environment}/{service}/{identifier}/{identifier_type}",
                self.present_time.strftime("%Y-%m-%dT%H:%M:%S"),
                ex=expiration_time,
            )

        pipe.execute()

    def check_incrementer(self, service: str, identifier: str) -> bool:
        increment_value = self.client.get(
            f"{service}/{self.present_time.strftime('%Y-%m-%d')}/{identifier}"
        )

        return bool(increment_value) and (int(increment_value) >= self.oclc_limit)

    def set_incrementer(self, service: str, identifier: str, amount: int = 1):
        key = f"{service}/{self.present_time.strftime('%Y-%m-%d')}/{identifier}"

        self.client.incr(key, amount=amount)
