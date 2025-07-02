from pymarc import parse_xml_to_array

from mappings.marc_record import map_marc_record
from model import Source


def test_map_marc_record():
    with open("tests/fixtures/grin-mets.xml", "rb") as metadata_file:
        marc_records = parse_xml_to_array(metadata_file)

    record = map_marc_record(marc_records[0], source=Source.GRIN, pdf_url="https://drb-files-limited-qa.s3.amazonaws.com/tagged_pdfs/990021264030302486.pdf")
    
    assert record.source == Source.GRIN.value
    assert record.source_id == "990021264030302486|grin"
    assert (
        record.title == "Twelve years a slave : narrative of Solomon Northup, a citizen of New-York, kidnapped in Washington City in 1841, and rescued in 1853, from a cotton plantation near the Red River in Louisiana"
    )
    assert record.authors == ['Northup, Solomon, 1808-1863?|||true']
    assert record.identifiers == ['990021264030302486|grin', 'u397481|oclc', 'ocm57572322|oclc']
    assert record.dates == ['1853|publication_date']
    assert record.publisher == ['Derby and Miller||']
    assert record.has_part == [
        '1|https://drb-files-limited-qa.s3.amazonaws.com/tagged_pdfs/990021264030302486.pdf|grin|application/pdf|{"download": true}',
    ]
