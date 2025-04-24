from model import Source
from processes import CLACSOProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_clacso_process(mock_epub_to_webpub):
    clacso_process = CLACSOProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = clacso_process.runProcess()

    records = assert_ingested_records(sources=[Source.CLACSO.value], expected_number_of_records=number_of_records_ingested)

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=2)

    assert_uploaded_manifests(records)
