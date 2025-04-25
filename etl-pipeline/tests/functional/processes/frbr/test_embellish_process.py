import pytest

from processes import ClassifyProcess, CatalogProcess, RecordEmbellisher
from managers import RedisManager
from model import Record, RecordState


def test_embellish_record(db_manager, unembellished_record_uuid):
    redis_manager = RedisManager()

    redis_manager.create_client()
    redis_manager.clear_cache()

    classify_process = ClassifyProcess(None, None, None, unembellished_record_uuid)
    classify_process.runProcess()

    catalog_process = CatalogProcess(None, None, None, None)
    catalog_process.runProcess(max_attempts=1)

    assert_record_embellished(record_uuid=unembellished_record_uuid, db_manager=db_manager)


@pytest.mark.skip
def test_embellish_record(db_manager, unembellished_record_uuid):
    redis_manager = RedisManager()

    redis_manager.create_client()
    redis_manager.clear_cache()

    record_embellisher = RecordEmbellisher(db_manager=db_manager)

    unembellished_record = db_manager.session.query(Record).filter(Record.uuid == unembellished_record_uuid).first()

    record_embellisher.embellish_record(record=unembellished_record)

    assert_record_embellished(record_uuid=unembellished_record_uuid, db_manager=db_manager)

def assert_record_embellished(record_uuid: str, db_manager):
    embellished_record = db_manager.session.query(Record).filter(Record.uuid == record_uuid).first()
    db_manager.session.refresh(embellished_record)

    assert embellished_record.frbr_status == 'complete'

    classify_record = (
        db_manager.session.query(Record)
            .filter(
                Record.source == 'oclcClassify',
                Record.title.ilike(f"%{embellished_record.title}%"))
            .order_by(Record.date_created.desc())
            .first()
    )
    
    assert classify_record is not None

    oclc_identifiers = [id for id in classify_record.identifiers if id.endswith('|oclc')]

    catalog_records = (
        db_manager.session.query(Record)
            .filter(
                Record.source == 'oclcCatalog',
                Record.source_id.in_(oclc_identifiers)
            )
            .all()
    )
    
    assert len(catalog_records) == len(oclc_identifiers)
