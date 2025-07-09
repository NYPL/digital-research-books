from datetime import datetime
import pytest

from managers import RedisManager


class TestRedisManager:
    @pytest.fixture
    def test_instance(self, mocker):
        mocker.patch.dict(
            "os.environ",
            {"REDIS_HOST": "host", "REDIS_PORT": "port", "ENVIRONMENT": "testEnv"},
        )
        return RedisManager()

    def test_initializer(self, test_instance):
        assert test_instance.host == "host"
        assert test_instance.port == "port"

    def test_create_client(self, test_instance, mocker):
        mock_redis = mocker.patch("managers.redis.Redis")
        mock_redis.return_value = "test_client"

        test_instance.create_client()

        assert test_instance.client == "test_client"
        mock_redis.assert_called_once_with(host="host", port="port", socket_timeout=5)

    def test_check_or_set_key_found_recent(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = (
            datetime.now().strftime("%Y-%m-%dT%H:%M:%S").encode("utf-8")
        )

        mock_set = mocker.patch.object(RedisManager, "set_key")

        assert test_instance.check_or_set_key("test", 1, "test") is True
        test_instance.client.get.assert_called_once_with("testEnv/test/1/test")
        mock_set.assert_not_called

    def test_check_or_set_key_found_old(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = (
            datetime(1900, 1, 1).strftime("%Y-%m-%dT%H:%M:%S").encode("utf-8")
        )

        mock_set = mocker.patch.object(RedisManager, "set_key")

        assert test_instance.check_or_set_key("test", 1, "test") is False
        test_instance.client.get.assert_called_once_with("testEnv/test/1/test")
        mock_set.assert_called_once

    def test_check_or_set_key_not_found(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = None

        mock_set = mocker.patch.object(RedisManager, "set_key")

        assert test_instance.check_or_set_key("test", 1, "test") is False
        test_instance.client.get.assert_called_once_with("testEnv/test/1/test")
        mock_set.assert_called_once

    def test_multi_check_or_set_key(self, test_instance, mocker):
        mock_set = mocker.patch.object(RedisManager, "multi_set_key")

        test_instance.one_day_ago = datetime(2021, 1, 1)

        test_instance.client = mocker.MagicMock()
        test_instance.client.mget.return_value = [
            b"2022-01-01T00:00:00",
            b"2020-01-01T00:00:00",
        ]

        test_output = test_instance.multi_check_or_set_key("test", [1, 2], "test")

        assert test_output == [(1, False), (2, True)]

        mock_set.assert_called_once_with(
            "test", [2], "test", expiration_time=60 * 60 * 24 * 7
        )

    def test_set_key(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()

        test_instance.set_key(
            "test",
            1,
            "test",
        )

        test_instance.client.set.assert_called_once_with(
            "testEnv/test/1/test",
            test_instance.present_time.strftime("%Y-%m-%dT%H:%M:%S"),
            ex=604800,
        )

    def test_multi_set_key(self, test_instance, mocker):
        test_instance.environment = "testEnv"
        test_instance.present_time = datetime(2022, 1, 1, 0, 0, 0)

        test_pipe = mocker.MagicMock()
        test_instance.client = mocker.MagicMock()
        test_instance.client.pipeline.return_value = test_pipe

        test_instance.multi_set_key("test", [1, 2, 3], "test")

        test_pipe.set.assert_has_calls(
            [
                mocker.call(
                    "testEnv/test/1/test", "2022-01-01T00:00:00", ex=60 * 60 * 24 * 7
                ),
                mocker.call(
                    "testEnv/test/2/test", "2022-01-01T00:00:00", ex=60 * 60 * 24 * 7
                ),
                mocker.call(
                    "testEnv/test/3/test", "2022-01-01T00:00:00", ex=60 * 60 * 24 * 7
                ),
            ]
        )

        test_pipe.execute.assert_called_once()

    def test_check_incrementer_false(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = b"1"

        assert test_instance.check_incrementer("test", "id") == False

        test_instance.client.get.assert_called_once_with(
            f"test/{test_instance.present_time.strftime('%Y-%m-%d')}/id"
        )

    def test_check_incrementer_true(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = b"500001"

        assert test_instance.check_incrementer("test", "id") == True

        test_instance.client.get.assert_called_once_with(
            f"test/{test_instance.present_time.strftime('%Y-%m-%d')}/id"
        )

    def test_check_incrementer_none_false(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = None

        assert test_instance.check_incrementer("test", "id") == False

        test_instance.client.get.assert_called_once_with(
            f"test/{test_instance.present_time.strftime('%Y-%m-%d')}/id"
        )

    def test_set_incrementer(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()

        test_instance.set_incrementer("test", "id", amount=3)

        test_instance.client.incr.assert_called_once_with(
            f"test/{test_instance.present_time.strftime('%Y-%m-%d')}/id", amount=3
        )
