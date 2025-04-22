import json
import os
from time import sleep

from managers import RabbitMQManager
from model import Record
from processes.record_pipeline import RecordPipelineProcess
from tests.functional.processes.frbr.test_cluster_process import assert_record_clustered
from tests.functional.processes.frbr.test_embellish_process import assert_record_embellished


def test_record_pipeline(db_manager, rabbitmq_manager: RabbitMQManager, unembellished_pipeline_record_uuid):
    record_queue = os.environ['RECORD_PIPELINE_QUEUE']
    record_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']

    rabbitmq_manager.create_or_connect_queue(record_queue, record_route)

    record_pipeline = RecordPipelineProcess()

    record = db_manager.session.query(Record).filter(Record.uuid == unembellished_pipeline_record_uuid).first()

    rabbitmq_manager.send_message_to_queue(
        queue_name=record_queue,
        routing_key=record_route,
        message=json.dumps(record.to_dict(), default=str)
    )

    sleep(1)

    record_pipeline.runProcess(max_attempts=2)
    db_manager.session.refresh(record)

    assert_record_embellished(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
    assert_record_clustered(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
