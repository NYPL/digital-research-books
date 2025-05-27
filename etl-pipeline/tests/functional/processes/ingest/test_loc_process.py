from model import Source
from processes import IngestProcess, RecordFileSaver
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_uploaded_epubs import assert_uploaded_epubs


def test_loc_process(db_manager, s3_manager, mock_epub_to_webpub, mock_sqs_manager):
    loc_process = IngestProcess("complete", None, None, None, 5, None, Source.LOC.value)
    number_of_records_ingested = loc_process.runProcess()

    records = assert_ingested_records(
        db_manager,
        sources=[Source.LOC.value],
        expected_number_of_records=number_of_records_ingested,
    )

    record_file_saver = RecordFileSaver(
        db_manager=db_manager, storage_manager=s3_manager
    )

    for record in records:
        record_file_saver.save_record_files(record)

    assert_uploaded_manifests(records)
    assert_uploaded_epubs(records)
