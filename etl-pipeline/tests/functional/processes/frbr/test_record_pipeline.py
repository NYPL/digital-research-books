

import json
import os
from time import sleep
import pytest

from managers import RabbitMQManager
from processes.record_pipeline import RecordPipelineProcess
from tests.functional.processes.frbr.test_cluster_process import assert_record_clustered
from tests.functional.processes.frbr.test_frbr_process import assert_record_frbrized

@pytest.mark.skipif(
    os.getenv('ENVIRONMENT') == 'qa',
    reason="Skipping record pipeline test in QA environment"
)

def test_record_pipeline(db_manager, rabbitmq_manager: RabbitMQManager, unfrbrized_pipeline_record_uuid):
    record_queue = os.environ['RECORD_PIPELINE_QUEUE']
    record_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']

    rabbitmq_manager.create_or_connect_queue(record_queue, record_route)

    record_pipeline = RecordPipelineProcess()

    rabbitmq_manager.send_message_to_queue(
        queue_name=record_queue,
        routing_key=record_route,
        message={'sourceId': '1503292738|isbn', 'source': 'test_source'}
    )

    sleep(1)

    record_pipeline.runProcess(max_attempts=1)

    assert_record_frbrized(record_uuid=unfrbrized_pipeline_record_uuid, db_manager=db_manager)
    assert_record_clustered(record_uuid=unfrbrized_pipeline_record_uuid, db_manager=db_manager)
