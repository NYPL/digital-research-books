import json
import os

from model import Source
from processes import LOCProcess, RecordPipelineProcess
from .assert_ingested_records import assert_ingested_records
from .assert_uploaded_manifests import assert_uploaded_manifests
from .assert_uploaded_epubs import assert_uploaded_epubs


def test_loc_process(mock_record_pipeline_rabbitmq_manager):
    loc_process = LOCProcess('complete', None, None, None, 5, None)
    number_of_records_ingested = loc_process.runProcess()

    pipeline_queue_name = os.environ["RECORD_PIPELINE_QUEUE"]
    messages = mock_record_pipeline_rabbitmq_manager.queues[pipeline_queue_name]
    assert len(messages) == number_of_records_ingested
    assert all(
        json.loads(message)["source"] == Source.LOC.value
        for message in messages
    )
