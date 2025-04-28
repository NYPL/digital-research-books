from model import Record


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
