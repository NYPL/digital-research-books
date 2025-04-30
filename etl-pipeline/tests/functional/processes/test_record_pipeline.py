from model import Record
from processes.record_pipeline import RecordPipelineProcess
from .assert_record_clustered import assert_record_clustered


def test_record_pipeline(db_manager, unclustered_record_uuid, mock_epub_to_webpub):
    record_pipeline = RecordPipelineProcess()

    record = (
        db_manager.session.query(Record)
        .filter(Record.uuid == unclustered_record_uuid)
        .first()
    )

    record_pipeline.sqs_manager.send_message_to_queue(
        message={"source_id": record.source_id, "source": record.source}
    )

    record_pipeline.runProcess(max_attempts=1)

    assert_record_clustered(record_uuid=unclustered_record_uuid, db_manager=db_manager)
