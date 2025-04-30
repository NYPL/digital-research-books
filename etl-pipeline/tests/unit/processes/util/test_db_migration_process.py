from alembic import config

from processes import MigrationProcess


class TestDatabaseMigrationProcess:
    def test_run_process(self, mocker):
        mock_alembic = mocker.patch.object(config, "main")

        test_db_migration_process = MigrationProcess(
            None, None, None, None, None, None, ["testOpt1", "testOpt2"]
        )
        test_db_migration_process.runProcess()

        mock_alembic.assert_called_once_with(
            argv=["--raiseerr", "testOpt1", "testOpt2"]
        )
