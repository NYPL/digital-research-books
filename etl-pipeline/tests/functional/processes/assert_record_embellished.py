from model import Record


def assert_record_embellished(record_uuid: str, db_manager):
    embellished_record = (
        db_manager.session.query(Record).filter(Record.uuid == record_uuid).first()
    )
    db_manager.session.refresh(embellished_record)

    assert embellished_record.frbr_status == "complete"

    owi_number = next((id for id in embellished_record.identifiers if id.endswith('owi')), None)

    assert owi_number is not None

    catalog_records = (
        db_manager.session.query(Record)
            .filter(
                Record.source == 'oclcCatalog',
                Record.identifiers.overlap(embellished_record.identifiers)
            )
            .all()
    )
    
    assert len(catalog_records) > 0
