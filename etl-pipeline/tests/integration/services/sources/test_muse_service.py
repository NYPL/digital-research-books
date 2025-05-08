from services import MUSEService


def test_get_records():
    muse_service = MUSEService()

    records = muse_service.get_records(limit=10)

    for record in records:
        assert record is not None


def test_get_record():
    muse_service = MUSEService()

    record = muse_service.get_record(record_id="97971")

    assert record is not None
