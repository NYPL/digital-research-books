from processes.record_clusterer import RecordClusterer
from model import Record, Item, Edition, Work, RecordState
from .assert_record_clustered import assert_record_clustered


def test_cluster_record(db_manager, redis_manager, unclustered_pipeline_record_uuid):
    record_clusterer = RecordClusterer(db_manager=db_manager, redis_manager=redis_manager)

    unclustered_record = db_manager.session.query(Record).filter(Record.uuid == unclustered_pipeline_record_uuid).first()

    record_clusterer.cluster_record(unclustered_record)

    assert_record_clustered(record_uuid=unclustered_pipeline_record_uuid, db_manager=db_manager)


def test_cluster_multi_edition(db_manager, redis_manager, unclustered_multi_edition_uuid):
    record_clusterer = RecordClusterer(db_manager=db_manager, redis_manager=redis_manager)

    record_to_cluster = db_manager.session.query(Record).filter(Record.uuid == unclustered_multi_edition_uuid).first()

    record_clusterer.cluster_record(record_to_cluster)

    db_manager.session.refresh(record_to_cluster)

    assert record_to_cluster.cluster_status == True
    assert record_to_cluster.state == RecordState.CLUSTERED.value
    
    work = (
        db_manager.session.query(Work)
            .join(Edition, Work.id == Edition.work_id)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Item.record_id == record_to_cluster.id)
            .first()
    )

    editions = (
        db_manager.session.query(Edition)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Edition.work_id == work.id)
            .all()
    )
    
    assert work is not None

    assert len(editions) == 2

    for edition in editions:
        assert edition.work_id == work.id


def test_cluster_multi_item(db_manager, redis_manager, unclustered_multi_item_uuid):
    record_clusterer = RecordClusterer(db_manager=db_manager, redis_manager=redis_manager)

    record_to_cluster = db_manager.session.query(Record).filter(Record.uuid == unclustered_multi_item_uuid).first()

    record_clusterer.cluster_record(record_to_cluster)

    db_manager.session.refresh(record_to_cluster)

    assert record_to_cluster.cluster_status == True
    assert record_to_cluster.state == RecordState.CLUSTERED.value
    
    work = (
        db_manager.session.query(Work)
            .join(Edition, Work.id == Edition.work_id)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Item.record_id == record_to_cluster.id)
            .first()
    )

    editions = (
        db_manager.session.query(Edition)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Edition.work_id == work.id)
            .all()
    )
    
    items = (
        db_manager.session.query(Item)
            .filter(Item.edition_id == editions[0].id)
            .all()
    )
    
    assert work is not None

    assert len(editions) == 1

    assert editions[0].work_id == work.id

    assert len(items) == 2

    for item in items:
        assert item.edition_id == editions[0].id
