from model import Source
from processes import NYPLProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests

def test_nypl_process(mock_epub_to_webpub):
    nypl_process = NYPLProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = nypl_process.runProcess()

    records = assert_ingested_records(sources=[Source.NYPL.value], expected_number_of_records=number_of_records_ingested)

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=2)

    assert_uploaded_manifests(records)
