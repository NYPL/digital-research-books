import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import OperationalError

from managers import DBManager


class TestDBManager:
    @pytest.fixture
    def test_instance(self, mocker):
        mock_decrypt = mocker.patch.object(DBManager, "decrypt_env_var")
        mock_decrypt.side_effect = ["user", "pswd", "host", "port", "name"]

        return DBManager()

    def test_initializer(self, test_instance):
        assert test_instance.user == "user"
        assert test_instance.pswd == "pswd"
        assert test_instance.host == "host"
        assert test_instance.port == "port"
        assert test_instance.db == "name"
        assert test_instance.engine == None
        assert test_instance.session == None

    def test_generate_engine_success(self, test_instance, mocker):
        mock_create = mocker.patch("managers.db.create_engine")
        mock_create.return_value = "test_engine"

        test_instance.generate_engine()

        assert test_instance.engine == "test_engine"
        mock_create.assert_called_once_with("postgresql://user:pswd@host:port/name")

    def test_generate_engine_error(self, test_instance, mocker):
        mock_create = mocker.patch("managers.db.create_engine")
        mock_create.side_effect = Exception

        with pytest.raises(Exception):
            test_instance.generate_engine()

    def test_initialize_database_run(self, test_instance, mocker):
        test_instance.engine = mocker.MagicMock()
        test_instance.engine.dialect.has_table.return_value = None
        mock_base = mocker.patch("managers.db.Base")
        mock_has_table = mocker.MagicMock()
        mock_has_table.has_table.return_value = False
        mock_inspect = mocker.patch("managers.db.inspect")
        mock_inspect.return_value = mock_has_table

        test_instance.initialize_database()

        mock_inspect.assert_called_once_with(test_instance.engine)
        mock_has_table.has_table.assert_called_once_with("works")
        mock_base.metadata.create_all.assert_called_once_with(test_instance.engine)

    def test_initialize_database_skip(self, test_instance, mocker):
        test_instance.engine = mocker.MagicMock()
        test_instance.engine.dialect.has_table.return_value = "test_table"
        mock_base = mocker.patch("managers.db.Base")
        mock_has_table = mocker.MagicMock()
        mock_has_table.has_table.return_value = True
        mock_inspect = mocker.patch("managers.db.inspect")
        mock_inspect.return_value = mock_has_table

        test_instance.initialize_database()

        mock_inspect.assert_called_once_with(test_instance.engine)
        mock_has_table.has_table.assert_called_once_with("works")
        mock_base.metadata.create_all.assert_not_called()

    def test_create_session_engine_exists(self, test_instance, mocker):
        mock_sessionmaker = mocker.patch("managers.db.sessionmaker")
        mock_session_manager = mocker.MagicMock()
        mock_sessionmaker.return_value = mock_session_manager
        mock_session_manager.return_value = "test_session"
        test_instance.engine = "test_engine"

        test_instance.create_session()

        assert test_instance.session == "test_session"
        mock_sessionmaker.assert_called_once_with(bind="test_engine", autoflush=False)
        mock_session_manager.assert_called_once

    def test_create_session_engine_not_exists_autoflush(self, test_instance, mocker):
        mock_sessionmaker = mocker.patch("managers.db.sessionmaker")
        mock_session_manager = mocker.MagicMock()
        mock_sessionmaker.return_value = mock_session_manager
        mock_session_manager.return_value = "test_session"
        test_instance.engine = None

        mock_generate = mocker.patch.object(DBManager, "generate_engine")

        test_instance.create_session(autoflush=True)

        assert test_instance.session == "test_session"
        mock_generate.assert_called_once
        mock_sessionmaker.assert_called_once_with(bind=None, autoflush=True)

    def test_start_session(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()

        test_instance.start_session()

        test_instance.session.begin_nested.assert_called_once

    def test_commit_changes_success(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()

        test_instance.commit_changes()

        test_instance.session.commit.assert_called_once()

    def test_commit_changes_deadlock(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()
        test_instance.session.commit.side_effect = [
            OperationalError("test", "test", "test"),
            None,
        ]

        mock_rollback = mocker.patch.object(DBManager, "rollback_changes")

        test_instance.commit_changes()

        assert test_instance.session.commit.call_count == 2
        mock_rollback.assert_called_once()

    def test_commit_changes_deadlock_repeated(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()
        test_instance.session.commit.side_effect = OperationalError(
            "test", "test", "test"
        )

        mock_rollback = mocker.patch.object(DBManager, "rollback_changes")

        test_instance.commit_changes()

        assert test_instance.session.commit.call_count == 2
        assert mock_rollback.call_count == 2

    def test_rollback_changes(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()

        test_instance.rollback_changes()

        test_instance.session.rollback.assert_called_once

    def test_close_connection(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()
        test_instance.engine = mocker.MagicMock()
        mock_commit = mocker.patch.object(DBManager, "commit_changes")

        test_instance.close_connection()

        mock_commit.assert_called_once
        test_instance.session.close.assert_called_once
        test_instance.engine.dispose.assert_called_once

    def test_bulk_save_objects_default(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()

        test_instance.bulk_save_objects([1, 2, 3])

        test_instance.session.bulk_save_objects.assert_called_once_with(
            [1, 2, 3], update_changed_only=True
        )
        test_instance.session.commit.assert_called_once()
        test_instance.session.flush.assert_called_once()

    def test_bulk_save_objects_only_changed_false(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()

        test_instance.bulk_save_objects([1, 2, 3], only_changed=False)

        test_instance.session.bulk_save_objects.assert_called_once_with(
            [1, 2, 3], update_changed_only=False
        )

    def test_bulk_save_objects_readlock_retry(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()
        test_instance.session.bulk_save_objects.side_effect = [
            OperationalError("test", "test", "test"),
            None,
        ]

        mock_rollback = mocker.patch.object(DBManager, "rollback_changes")

        test_instance.bulk_save_objects([1, 2, 3])

        test_instance.session.bulk_save_objects.assert_has_calls(
            [
                mocker.call([1, 2, 3], update_changed_only=True),
                mocker.call([1, 2, 3], update_changed_only=True),
            ]
        )
        mock_rollback.assert_called_once()

    def test_bulk_save_objects_readlock_retry_fail(self, test_instance, mocker):
        test_instance.session = mocker.MagicMock()
        test_instance.session.bulk_save_objects.side_effect = OperationalError(
            "test", "test", "test"
        )

        mock_rollback = mocker.patch.object(DBManager, "rollback_changes")

        test_instance.bulk_save_objects([1, 2, 3], only_changed=False)

        test_instance.session.bulk_save_objects.assert_has_calls(
            [
                mocker.call([1, 2, 3], update_changed_only=False),
                mocker.call([1, 2, 3], update_changed_only=False),
            ]
        )
        assert mock_rollback.call_count == 2

    def test_decrypt_env_var_present(self, mocker):
        mocker.patch.dict("os.environ", {"test": "test_value"})

        decrypted_var = DBManager.decrypt_env_var("test")

        assert decrypted_var == "test_value"

    def test_decrypt_env_var_missing(self, mocker):
        mocker.patch.dict("os.environ", {"test": "test_value"})

        decrypted_var = DBManager.decrypt_env_var("other")

        assert decrypted_var == None
