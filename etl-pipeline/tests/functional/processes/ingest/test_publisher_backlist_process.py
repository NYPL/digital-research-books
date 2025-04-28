import pytest

from model import Source
from processes import PublisherBacklistProcess, RecordFileSaver
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_files import assert_uploaded_files


@pytest.mark.skip
def test_publisher_backlist_process(db_manager, s3_manager, mock_epub_to_webpub, mock_sqs_manager):
    publisher_backlist_process = PublisherBacklistProcess('complete', None, None, None, 1, None)
    number_of_records_ingested = publisher_backlist_process.runProcess()

    records = assert_ingested_records(
        db_manager,
        sources=[Source.SCHOMBURG.value, Source.U_OF_MICHIGAN_BACKLIST.value],
        expected_number_of_records=number_of_records_ingested
    )

    record_file_saver = RecordFileSaver(db_manager=db_manager, storage_manager=s3_manager)

    for record in records:
        record_file_saver.save_record_files(record)

    assert_uploaded_files(records)
