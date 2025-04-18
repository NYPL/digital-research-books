from model import Source
from processes import LOCProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_uploaded_epubs import assert_uploaded_epubs


def test_loc_process():
    loc_process = LOCProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = loc_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=1)

    records = assert_ingested_records(sources=[Source.LOC.value], expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
    assert_uploaded_epubs(records)
