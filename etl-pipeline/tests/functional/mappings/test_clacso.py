from mappings.clacso import CLACSOMapping
from model import Source
from lxml import etree

from services.sources.dspace_service import DSpaceService


def test_map_clacso_record():
    with open("tests/fixtures/test-clacso.xml") as f:
        oaidc_records = etree.parse(f)

    for record in oaidc_records.xpath(
        ".//record", namespaces=DSpaceService.OAI_NAMESPACES
    ):
        if record is None:
            continue

        parsed_record = CLACSOMapping(
            clacso_record=record, namespaces=DSpaceService.OAI_NAMESPACES
        ).record
        assert parsed_record.source == Source.CLACSO.value
        assert parsed_record.source_id == "16936|clacso"
        assert (
            parsed_record.title
            == "La retracción del derecho a la educación en el marco de las restauraciones conservadoras : una mirada nuestroamericana"
        )
