from services import METService


def test_get_records():
    met_service = METService()

    records = met_service.get_records(limit=100)

    for record in records:
        assert record is not None
