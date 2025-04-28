from processes import DOABProcess, RecordPipelineProcess
from model import Source
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_uploaded_epubs import assert_uploaded_epubs

def test_doab_process(mock_epub_to_webpub):
    doab_process = DOABProcess('complete', None, None, None, 1, None)
    number_of_records_ingested = doab_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=1)

    records = assert_ingested_records(sources=[Source.DOAB.value], expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
    assert_uploaded_epubs(records)
