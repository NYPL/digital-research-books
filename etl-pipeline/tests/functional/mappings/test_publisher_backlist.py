import json
import os

from digital_assets import get_stored_file_url
from mappings.publisher_backlist import PublisherBacklistMapping
from model import Part, FileFlags


def test_map_loc_record():
    with open('tests/fixtures/test-publisher-backlist-record.json') as f:
        publisher_backlist_data = json.load(f)

    record_mapping = PublisherBacklistMapping(publisher_backlist_data)
    record_mapping.applyMapping()
    record = record_mapping.record

    destination_storage_location = get_stored_file_url(storage_name=os.environ['FILE_BUCKET'], file_path='')
    source_storage_location = os.environ['PDF_BUCKET']

    assert record is not None
    assert record.title == "Aunt Phillis's cabin; or, Southern life as it is / by Mary H. Eastman."
    assert record.source_id == 'recBrlpABSQOkRc4m'
    assert record.identifiers == ['2510423|oclc', 'uc1.b4102944|hathi']
    assert record.authors == ['Eastman, Mary H. 1818-1887.|||true']
    assert record.publisher == ['University Libraries||']
    assert record.rights == 'SCH Collection/Hathi files|public_domain||Public Domain|'
    assert record.has_part == [
        str(Part(
            index=1, 
            url=f'{destination_storage_location}titles/publisher_backlist/Schomburg/uc1.b4102944/uc1.b4102944.pdf', 
            source='SCH Collection/Hathi files',
            file_type='application/pdf',
            flags=str(FileFlags(download=True)),
            source_url=f'https://{source_storage_location}.s3.amazonaws.com/tagged_pdfs/uc1.b4102944.pdf')
        ),
        str(Part(
            index=None,
            url=f'{destination_storage_location}covers/publisher_backlist/hathi_uc1.b4102944.png',
            source='SCH Collection/Hathi files',
            file_type='image/png',
            flags=str(FileFlags(cover=True))
        ))
    ]
