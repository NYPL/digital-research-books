from processes import IngestProcess, RecordFileSaver
from model import Source
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_uploaded_epubs import assert_uploaded_epubs


def test_doab_process(db_manager, s3_manager, mock_epub_to_webpub, mock_sqs_manager):
    doab_process = IngestProcess("complete", None, None, None, 1, None, Source.DOAB.value)
    number_of_records_ingested = doab_process.runProcess()

    records = assert_ingested_records(
        db_manager,
        sources=[Source.DOAB.value],
        expected_number_of_records=number_of_records_ingested,
    )

    record_file_saver = RecordFileSaver(
        db_manager=db_manager, storage_manager=s3_manager
    )

    for record in records:
        record_file_saver.save_record_files(record)

    assert_uploaded_manifests(records)
    assert_uploaded_epubs(records)
