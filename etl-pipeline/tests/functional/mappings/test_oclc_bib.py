import json

from mappings.oclc_bib import map_oclc_record
from model import Source

# TODO: make assertions
def test_map_oclc_records():
    with open('tests/fixtures/test-oclc.json') as f:
        oclc_bib = json.load(f)

        oclc_record = map_oclc_record(oclc_bib)

        assert oclc_record is not None
        assert oclc_record.source == Source.MET.value
        assert oclc_record.source_id == '2235|met'
        assert oclc_record.title == 'Constitution and by-laws, 1870'
