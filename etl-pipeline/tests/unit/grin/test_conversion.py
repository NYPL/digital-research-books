from unittest.mock import MagicMock, patch
import pytest
from model import GRINState
from processes.grin.conversion import GRINConversion


@pytest.fixture
def mocked_conversion_process():
    with patch("processes.grin.conversion.GRINClient") as MockGrinClient:
        with patch("processes.grin.conversion.DBManager") as MockDBManager:
            with patch("managers.SQSManager") as MockSQSManager:
                with patch(
                    "processes.grin.conversion.utils.parse_process_args"
                ) as mock_parse_args:
                    mock_grin_client_instance = MockGrinClient.return_value
                    mock_sqs_manager_instance = MockSQSManager.return_value
                    mock_params = MagicMock(process_type="daily")
                    mock_parse_args.return_value = mock_params

                    mock_db_manager_instance = MagicMock()
                    MockDBManager.return_value.__enter__.return_value = (
                        mock_db_manager_instance
                    )

                    # Patch GRINConversion.__init__ to allow dependency injection
                    def test_init(self, client, db_manager, sqs_manager=None, **kwargs):
                        self.client = client
                        self.db_manager = db_manager
                        self.sqs_manager = sqs_manager
                        self.logger = MagicMock()
                        self.batch_limit = kwargs.get("batch_limit", 1000)
                        self.params = mock_params

                    with patch.object(GRINConversion, "__init__", test_init):
                        conversion_process_instance = GRINConversion(
                            client=mock_grin_client_instance,
                            db_manager=mock_db_manager_instance,
                            sqs_manager=mock_sqs_manager_instance,
                        )
                        yield conversion_process_instance, mock_db_manager_instance


def test_convert_barcodes_pending_conversion_fix(mocked_conversion_process):
    conversion_process_instance, mock_db_manager_instance = mocked_conversion_process

    mocked_in_process_data = ["3433124920790", "3433124920791", "3433124920792"]
    conversion_process_instance.client.in_process.return_value = mocked_in_process_data

    mocked_pending_barcodes = ["PENDING001", "PENDING002", "PENDING003"]
    mock_db_manager_instance.session.execute.return_value.scalars.return_value.all.return_value = mocked_pending_barcodes

    mock_converting = ["PENDING001"]
    mock_converted = ["PENDING003"]
    mock_unavailable = ["PENDING002"]

    with patch.object(
        conversion_process_instance,
        "_convert_barcodes",
        return_value=(mock_converting, mock_converted, mock_unavailable),
    ) as mock_convert_barcodes:
        with patch.object(
            conversion_process_instance, "_update_grin_state"
        ) as mock_update_grin_state:
            conversion_process_instance.convert_barcodes_pending_conversion()

            conversion_process_instance.client.in_process.assert_called_once()
            mock_db_manager_instance.session.execute.assert_called_once()
            mock_convert_barcodes.assert_called_once_with(mocked_pending_barcodes)

            assert mock_update_grin_state.call_count == 3
            mock_update_grin_state.assert_any_call(
                mock_converting,
                old_state=GRINState.PENDING_CONVERSION,
                new_state=GRINState.CONVERTING,
            )
            mock_update_grin_state.assert_any_call(
                mock_converted,
                old_state=GRINState.CONVERTING,
                new_state=GRINState.CONVERTED,
            )
            mock_update_grin_state.assert_any_call(
                mock_unavailable,
                old_state=GRINState.PENDING_CONVERSION,
                new_state=GRINState.UNAVAILABLE,
            )
