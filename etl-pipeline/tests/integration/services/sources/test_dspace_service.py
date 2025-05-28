from model import Source
from services import get_source_service


def test_get_records():
    dspace_service = get_source_service(Source.CLACSO.value)
    records = dspace_service.get_records(limit=5)

    for record in records:
        assert record is not None
