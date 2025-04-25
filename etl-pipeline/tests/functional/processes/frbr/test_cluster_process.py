import pytest
from processes import ClusterProcess, RecordClusterer
from model import Record, Item, Edition, Work, RecordState

def test_cluster_process(db_manager, unclustered_record_uuid):
    cluster_process = ClusterProcess('complete', None, None, unclustered_record_uuid, None)

    unclustered_record = db_manager.session.query(Record).filter(Record.uuid == unclustered_record_uuid).first()

    assert unclustered_record.cluster_status == False

    cluster_process.runProcess()
    
    db_manager.session.commit()

    assert_record_clustered(record_uuid=unclustered_record_uuid, db_manager=db_manager)

@pytest.mark.skip
def test_cluster_record(db_manager, unclustered_pipeline_record_uuid):
    record_clusterer = RecordClusterer(db_manager=db_manager)

    unclustered_record = db_manager.session.query(Record).filter(Record.uuid == unclustered_pipeline_record_uuid).first()

    record_clusterer.cluster_record(unclustered_record)

    assert_record_clustered(record_uuid=unclustered_pipeline_record_uuid, db_manager=db_manager)

def test_cluster_multi_edition(db_manager, unclustered_multi_edition_uuid):
    cluster_process = ClusterProcess('complete', None, None, unclustered_multi_edition_uuid, None)

    cluster_process.runProcess()
    
    db_manager.session.commit()

    clustered_record = db_manager.session.query(Record).filter(Record.uuid == unclustered_multi_edition_uuid).first()

    assert clustered_record.cluster_status == True
    
    work = (
        db_manager.session.query(Work)
            .join(Edition, Work.id == Edition.work_id)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Item.record_id == clustered_record.id)
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


def test_cluster_multi_item(db_manager, unclustered_multi_item_uuid):
    cluster_process = ClusterProcess('complete', None, None, unclustered_multi_item_uuid, None)

    cluster_process.runProcess()
    
    db_manager.session.commit()

    clustered_record = db_manager.session.query(Record).filter(Record.uuid == unclustered_multi_item_uuid).first()

    assert clustered_record.cluster_status == True
    
    work = (
        db_manager.session.query(Work)
            .join(Edition, Work.id == Edition.work_id)
            .join(Item, Edition.id == Item.edition_id)
            .filter(Item.record_id == clustered_record.id)
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

def assert_record_clustered(record_uuid: str, db_manager):
    clustered_record = db_manager.session.query(Record).filter(Record.uuid == record_uuid).first()

    assert clustered_record.cluster_status == True
    
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
