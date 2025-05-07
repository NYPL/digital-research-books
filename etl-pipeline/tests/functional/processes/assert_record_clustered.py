from model import Record, RecordState, Item, Edition, Work


def assert_record_clustered(record_uuid: str, db_manager):
    clustered_record = (
        db_manager.session.query(Record).filter(Record.uuid == record_uuid).first()
    )
    db_manager.session.refresh(clustered_record)

    assert clustered_record.cluster_status == True
    assert clustered_record.state == RecordState.CLUSTERED.value

    frbrized_model = (
        db_manager.session.query(Item, Edition, Work)
        .join(Edition, Edition.id == Item.edition_id)
        .join(Work, Work.id == Edition.work_id)
        .filter(Item.record_id == clustered_record.id)
        .first()
    )

    item, edition, work = frbrized_model

    assert item is not None
    assert edition is not None
    assert work is not None
