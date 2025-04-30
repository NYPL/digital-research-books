from datetime import datetime, timedelta


from services import HathiTrustService


def test_get_records():
    hathi_trust_service = HathiTrustService()

    records = hathi_trust_service.get_records(
        limit=5, start_timestamp=datetime.now() - timedelta(days=7)
    )

    for record in records:
        assert record is not None
