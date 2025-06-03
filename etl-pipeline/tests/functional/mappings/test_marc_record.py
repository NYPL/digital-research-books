from pymarc import parse_xml_to_array

from mappings.marc_record import map_marc_record
from model import Source

def test_map_marc_record():
    with open('tests/fixtures/grin-mets.xml', 'rb') as metadata_file:
        marc_records = parse_xml_to_array(metadata_file)

    record = map_marc_record(marc_records[0], source=Source.GRIN)

    # TODO: add verbose assertions
    assert record is not None
    assert record.title is not None
