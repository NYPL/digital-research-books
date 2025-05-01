import json

from mappings.oclc_bib import map_oclc_record
from model import Source

# TODO: make assertions
def test_map_oclc_records():
    with open('tests/fixtures/test-oclc.json') as f:
        oclc_bib = json.load(f)

        oclc_record = map_oclc_record(oclc_bib)

        assert oclc_record is not None
        assert oclc_record.source == Source.OCLC_CATALOG.value
        assert oclc_record.source_id == '903081363|oclc'
        assert oclc_record.title == 'Emma / by Jane Austen'
        assert oclc_record.authors == ['Austen, Jane|||true']
        assert oclc_record.identifiers == ['903081363|oclc', '4820344140|owi', '815036d083b6a40b5f7f36db29c01bbb|oec']
        assert oclc_record.dates == ['1892|publication_date']
        assert oclc_record.publisher == ['Little, Brown||']
        assert oclc_record.languages == ['||eng']
        assert oclc_record.has_part == ['1|http://books.google.com/books?id=vukYAAAAYAAJ|oclc|text/html|{"embed": true}', '1|http://books.google.com/books?id=pusYAAAAYAAJ|oclc|text/html|{"embed": true}', '1|http://catalog.hathitrust.org/api/volumes/oclc/9396259.html|oclc|text/html|{"embed": true}']

