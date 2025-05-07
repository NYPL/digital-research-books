from datetime import datetime, timezone, timedelta
from typing import Optional

from managers import DBManager
from model import Record


def assert_ingested_records(
    db_manager, sources: list[str], expected_number_of_records: Optional[int] = None
) -> list[Record]:
    records = (
        db_manager.session.query(Record)
        .filter(Record.source.in_(sources))
        .filter(
            Record.date_modified
            > datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
        )
        .all()
    )

    if expected_number_of_records is not None:
        assert len(records) >= expected_number_of_records, (
            f"Expected {expected_number_of_records} records, found {len(records)}"
        )
    else:
        assert len(records) >= 1

    return records
