import pytest
from sqlalchemy import text
from unittest.mock import patch

from processes import DatabaseMaintenanceProcess


class TestDatabaseMaintenanceProcess:
    @pytest.fixture
    def db_maintenance_process(self) -> DatabaseMaintenanceProcess:
        with patch("processes.util.db_maintenance.DBManager"):
            return DatabaseMaintenanceProcess()

    def test_run_process(
        self, db_maintenance_process: DatabaseMaintenanceProcess, mocker
    ):
        mock_connection_enter = mocker.MagicMock()
        mock_connection = mocker.MagicMock()
        db_maintenance_process.db_manager.engine.connect.return_value = (
            mock_connection_enter
        )
        mock_connection_enter.__enter__.return_value = mock_connection

        db_maintenance_process.runProcess()

        db_maintenance_process.db_manager.create_session.assert_called_once()

        mock_connection.execution_options.assert_called_once_with(
            isolation_level="AUTOCOMMIT"
        )

        assert mock_connection.execute.call_count == len(
            db_maintenance_process.VACUUMING_TABLES
        )

        db_maintenance_process.db_manager.close_connection.assert_called_once()
