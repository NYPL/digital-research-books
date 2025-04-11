from processes import HathiTrustProcess, RecordPipelineProcess
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_ingested_records import assert_ingested_records


def test_hathi_trust_process():
    hathi_trust_process = HathiTrustProcess('weekly', None, None, None, 5, None)
    number_of_records_ingested = hathi_trust_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=1)

    records = assert_ingested_records(source_name='hathitrust', expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
