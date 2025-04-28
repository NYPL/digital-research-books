from model import Record
from processes.record_pipeline import RecordPipelineProcess
from .assert_record_clustered import assert_record_clustered
from .assert_record_embellished import assert_record_embellished


def test_record_pipeline(db_manager, unembellished_pipeline_record_uuid, mock_epub_to_webpub):
    record_pipeline = RecordPipelineProcess()

    record = db_manager.session.query(Record).filter(
        Record.uuid == unembellished_pipeline_record_uuid,
    ).first()

    record_pipeline.sqs_manager.send_message_to_queue(
        message={ "source_id": record.source_id, "source": record.source }
    )

    record_pipeline.runProcess(max_attempts=1)

    assert_record_embellished(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
    assert_record_clustered(record_uuid=unembellished_pipeline_record_uuid, db_manager=db_manager)
