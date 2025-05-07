from processes.record_embellisher import RecordEmbellisher
from model import Record
from .assert_record_embellished import assert_record_embellished


def test_embellish_record(db_manager, redis_manager, unembellished_record_uuid):
    record_embellisher = RecordEmbellisher(
        db_manager=db_manager, redis_manager=redis_manager
    )

    unembellished_record = (
        db_manager.session.query(Record)
        .filter(Record.uuid == unembellished_record_uuid)
        .first()
    )

    record_embellisher.embellish_record(record=unembellished_record)

    assert_record_embellished(
        record_uuid=unembellished_record_uuid, db_manager=db_manager
    )
