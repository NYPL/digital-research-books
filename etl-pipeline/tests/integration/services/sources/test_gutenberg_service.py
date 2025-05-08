from services.sources.gutenberg_service import GutenbergService


def test_get_records():
    gutenberg_service = GutenbergService()

    records = gutenberg_service.get_records(limit=100)

    for record in records:
        assert record is not None
