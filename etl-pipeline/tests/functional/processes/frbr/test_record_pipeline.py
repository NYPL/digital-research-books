import json
from time import sleep

from managers import RedisManager
from model import Record
from processes.record_pipeline import RecordPipelineProcess
from tests.functional.processes.frbr.test_cluster_process import assert_record_clustered
from tests.functional.processes.frbr.test_embellish_process import assert_record_embellished


def test_record_pipeline(db_manager, unembellished_pipeline_record_uuid, mock_epub_to_webpub):
    redis_manager = RedisManager()

    redis_manager.create_client()
    redis_manager.clear_cache()

    record_pipeline = RecordPipelineProcess()

    record = db_manager.session.query(Record).filter(
        Record.uuid == unembellished_pipeline_record_uuid,
    ).first()

    record_pipeline.sqs_manager.send_message_to_queue(
        message={ "source_id": record.source_id, "source": record.source }
    )

    sleep(1)

    record_pipeline.runProcess(max_attempts=2)

    assert_record_embellished(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
    assert_record_clustered(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
