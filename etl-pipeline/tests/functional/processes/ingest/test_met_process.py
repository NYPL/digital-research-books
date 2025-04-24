from model import Source
from processes import METProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests


def test_met_process(mock_epub_to_webpub):
    met_process = METProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = met_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=2)
    
    records = assert_ingested_records(sources=[Source.MET.value], expected_number_of_records=number_of_records_ingested)
    assert_uploaded_manifests(records)
