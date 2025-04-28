from model import Source
from processes import HathiTrustProcess, RecordPipelineProcess
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_ingested_records import assert_ingested_records


def test_hathi_trust_process(mock_epub_to_webpub):
    hathi_trust_process = HathiTrustProcess('weekly', None, None, None, 5, None)
    number_of_records_ingested = hathi_trust_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=1)

    records = assert_ingested_records(sources=[Source.HATHI.value], expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
