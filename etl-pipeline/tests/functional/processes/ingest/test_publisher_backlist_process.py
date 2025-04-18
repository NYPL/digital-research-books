from model import Source
from processes import PublisherBacklistProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_files import assert_uploaded_files


def test_publisher_backlist_process():
    publisher_backlist_process = PublisherBacklistProcess('complete', None, None, None, 1, None)
    
    number_of_records_ingested = publisher_backlist_process.runProcess()

    record_pipeline_process = RecordPipelineProcess()
    record_pipeline_process.runProcess(max_attempts=2)

    records = assert_ingested_records(
        sources=[Source.SCHOMBURG.value, Source.U_OF_MICHIGAN_BACKLIST.value], 
        expected_number_of_records=number_of_records_ingested
    )
    assert_uploaded_files(records)
