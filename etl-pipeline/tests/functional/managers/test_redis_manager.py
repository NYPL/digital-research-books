from pottery import Redlock
from uuid import uuid4

from managers.redis import RedisManager

def test_any_lock():
    test_key = str(uuid4())
    redis_manager = RedisManager()
    redis_manager.create_client()

    lock = Redlock(key=test_key, masters={redis_manager.client})

    with lock:
        assert redis_manager.any_locked([test_key]) is True
