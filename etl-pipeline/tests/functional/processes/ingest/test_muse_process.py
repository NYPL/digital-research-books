from model import Source
from processes import MUSEProcess
from processes.record_pipeline import RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_files import assert_uploaded_files


def test_muse_process(mock_epub_to_webpub):
    muse_process = MUSEProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = muse_process.runProcess()

    records = assert_ingested_records(sources=[Source.MUSE.value], expected_number_of_records=number_of_records_ingested)

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=2)

    assert_uploaded_files(records)
