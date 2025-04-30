from datetime import datetime, timezone, timedelta
import pytest
from typing import Optional

import processes.utils as utils


@pytest.mark.parametrize(
    "process_type, ingest_period, expected_start_datetime",
    [
        (
            "daily",
            None,
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24),
        ),
        (
            "weekly",
            None,
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=7),
        ),
        (
            "custom",
            "2022-01-01T00:00:00",
            datetime.strptime("2022-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
        ),
        ("complete", None, None),
    ],
)
def test_get_start_datetime(
    process_type: str,
    ingest_period: Optional[str],
    expected_start_datetime: Optional[datetime],
):
    start_datetime = utils.get_start_datetime(
        process_type=process_type, ingest_period=ingest_period
    )

    if expected_start_datetime is not None:
        assert start_datetime is not None
        assert start_datetime.replace(
            minute=0, second=0, microsecond=0
        ) == expected_start_datetime.replace(minute=0, second=0, microsecond=0)
    else:
        assert start_datetime == expected_start_datetime


@pytest.mark.parametrize(
    "args, expected_params",
    [
        (("weekly",), utils.ProcessParams(process_type="weekly")),
        (("daily", None, None, None), utils.ProcessParams(process_type="daily")),
        (
            ("daily", "custom_file", "2022-01-01T00:00:00", "uuid"),
            utils.ProcessParams(
                process_type="daily",
                custom_file="custom_file",
                ingest_period="2022-01-01T00:00:00",
                record_id="uuid",
            ),
        ),
        (
            ("daily", None, None, None, 5000, 5000, "hathitrust"),
            utils.ProcessParams(
                process_type="daily", limit=5000, offset=5000, source="hathitrust"
            ),
        ),
    ],
)
def test_parse_process_args(args, expected_params: utils.ProcessParams):
    params = utils.parse_process_args(*args)

    assert params == expected_params
