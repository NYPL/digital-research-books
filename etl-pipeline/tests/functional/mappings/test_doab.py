from mappings.doab import DOABMapping
from model import Source
from lxml import etree

from services.sources.dspace_service import DSpaceService


def test_map_doab_record():
    with open("tests/fixtures/test-doab.xml") as f:
        oaidc_records = etree.parse(f)

    for record in oaidc_records.xpath(
        ".//record", namespaces=DSpaceService.OAI_NAMESPACES
    ):
        if record is None:
            continue

        parsed_record = DOABMapping(
            doab_record=record, namespaces=DSpaceService.OAI_NAMESPACES
        ).record
        assert parsed_record.source == Source.DOAB.value
        assert parsed_record.source_id == "20.500.12854/62823"
        assert parsed_record.title == "A World of Nourishment"


def test_map_deleted_doab_record():
    with open("tests/fixtures/test-doab-deleted.xml") as f:
        oaidc_records = etree.parse(f)

    for record in oaidc_records.xpath(
        ".//record", namespaces=DSpaceService.OAI_NAMESPACES
    ):
        if record is None:
            continue

        parsed_record = DOABMapping(
            doab_record=record, namespaces=DSpaceService.OAI_NAMESPACES
        ).record
        assert parsed_record.source == Source.DOAB.value
        assert parsed_record.source_id == "20.500.12854/54763"
        assert parsed_record._deleted_flag == True
