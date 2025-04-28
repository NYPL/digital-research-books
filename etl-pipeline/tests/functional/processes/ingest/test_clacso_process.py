from model import Source
from processes import CLACSOProcess, RecordFileSaver
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_clacso_process(db_manager, s3_manager, mock_epub_to_webpub, mock_sqs_manager):
    clacso_process = CLACSOProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = clacso_process.runProcess()

    records = assert_ingested_records(db_manager, sources=[Source.CLACSO.value], expected_number_of_records=number_of_records_ingested)

    record_file_saver = RecordFileSaver(db_manager=db_manager, storage_manager=s3_manager)

    for record in records:
        record_file_saver.save_record_files(record)

    assert_uploaded_manifests(records)
