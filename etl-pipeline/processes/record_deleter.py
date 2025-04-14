import os
from sqlalchemy.orm import joinedload

from logger import create_log
from model import Edition, Item, Record, Work
from managers import DBManager, ElasticsearchManager, S3Manager

logger = create_log(__name__)


class RecordDeleter:

    def __init__(self, db_manager: DBManager, store_manager: S3Manager, es_manager: ElasticsearchManager):
        self.db_manager = db_manager
        self.store_manager = store_manager
        self.es_manager = es_manager

    def delete_record(self, record: Record):
        self._delete_record_digital_assets(record)
        self._update_frbr_model(record)

        self.db_manager.session.delete(record)
        self.db_manager.session.commit()

        logger.info(f'Deleted {record}')

    def _update_frbr_model(self, record: Record):
        items = self.db_manager.session.query(Item).filter(Item.record_id == record.id).all()
        edition_ids = { item.edition_id for item in items }

        for item in items:
            self.db_manager.session.delete(item)

        self.db_manager.session.commit()

        deleted_edition_ids = {}
        deleted_work_ids = set()
        work_ids = set()
        work_ids_to_uuids = {}

        for edition_id in edition_ids:
            edition = (
                self.db_manager.session.query(Edition)
                    .options(joinedload(Edition.items))
                    .filter(Edition.id == edition_id)
                    .first()
            )

            if edition and not edition.items:
                self.db_manager.session.delete(edition)

                work_ids.add(edition.work_id)
                deleted_edition_ids[edition_id] = edition.work_id

        self.db_manager.session.commit()

        for work_id in work_ids:
            work = (
                self.db_manager.session.query(Work)
                    .options(joinedload(Work.editions))
                    .filter(Work.id == work_id)
                    .first()
            )
            work_ids_to_uuids[work.id] = work.uuid

            if work and not work.editions:
                self.db_manager.session.delete(work)
                self.es_manager.client.delete(index=os.environ["ELASTICSEARCH_INDEX"], id=work.uuid)

                deleted_work_ids.add(work_id)

        self.db_manager.session.commit()

        for edition_id, work_id in deleted_edition_ids.items():
            if work_id not in deleted_work_ids:
                work_uuid = work_ids_to_uuids[work_id]
                work_document = self.es_manager.client.get(index=os.environ["ELASTICSEARCH_INDEX"], id=work_uuid)
                editions = work_document["_source"].get("editions", [])

                updated_editions = [edition for edition in editions if edition.get("id") != edition_id]
                work_document["_source"]["editions"] = updated_editions

                self.es_manager.client.index(
                    index=os.environ["ELASTICSEARCH_INDEX"],
                    id=work_uuid,
                    body=work_document["_source"],
                )

    def _delete_record_digital_assets(self, record: Record):
        for part in record.parts:
            if part.file_bucket and part.file_key:
                self.store_manager.client.delete_object(Bucket=part.file_bucket, Key=part.file_key)
