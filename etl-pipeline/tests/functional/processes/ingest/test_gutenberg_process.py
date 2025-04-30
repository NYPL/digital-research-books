from model import RecordState, Source
from processes import RecordFileSaver, GutenbergProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_gutenberg_process(
    db_manager, s3_manager, mock_epub_to_webpub, mock_sqs_manager
):
    gutenberg_process = GutenbergProcess("complete", None, None, None, 5, None)
    number_of_records_ingested = gutenberg_process.runProcess()

    records = assert_ingested_records(
        db_manager,
        sources=[Source.GUTENBERG.value],
        expected_number_of_records=number_of_records_ingested,
    )

    record_file_saver = RecordFileSaver(
        db_manager=db_manager, storage_manager=s3_manager
    )

    for record in records:
        succeeded_records = []
        failed_records = []
        try:
            record_file_saver.save_record_files(record)
        except Exception:
            failed_records.append(record)
        else:
            succeeded_records.append(record)

    assert_uploaded_manifests(succeeded_records)
    assert all(record.state == RecordState.INGESTED for record in failed_records)
