from processes import LOCProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_loc_process():
    loc_process = LOCProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = loc_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=1)

    records = assert_ingested_records(source_name='loc', expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
