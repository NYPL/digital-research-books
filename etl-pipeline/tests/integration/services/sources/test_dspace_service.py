from mappings.doab import DOABMapping
from processes import CLACSOProcess
from services.sources.dspace_service import DSpaceService


def test_get_records():
    dspace_service = DSpaceService(
        base_url=CLACSOProcess.CLACSO_BASE_URL, source_mapping=DOABMapping
    )

    records = dspace_service.get_records(limit=5)

    for record in records:
        assert record is not None
