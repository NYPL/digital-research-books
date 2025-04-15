import re
from sqlalchemy.exc import DataError
from logger import create_log
from managers import (
    DBManager,
    ElasticsearchManager,
    KMeansManager,
    SFRElasticRecordManager,
    SFRRecordManager,
)
from constants.get_constants import get_constants
from model import Record, Work
from .record_blocking import RecordBlocker


logger = create_log(__name__)


class RecordClusterer:
    """Clusters related bibliographic records and transforms them into FRBR model objects.
    
    This class handles the process of:
    1. Finding related records through identifier and title matching (blocking)
    2. Clustering those records using machine learning
    3. Creating Work/Edition/Item objects from the clusters
    4. Managing database updates and search indexing
    """

    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager
        self.blocker = RecordBlocker(db_manager=db_manager)

        self.elastic_search_manager = ElasticsearchManager()

        self.elastic_search_manager.create_elastic_connection()
        self.elastic_search_manager.create_elastic_search_ingest_pipeline()
        self.elastic_search_manager.create_elastic_search_index()

        self.constants = get_constants()

    def cluster_record(self, record) -> list[Record]:
        """Clusters a single record and updates the database and Elasticsearch.
        """
        try:
            work, stale_work_ids, records = self._get_clustered_work_and_records(record)
            self._commit_changes()

            self._delete_stale_works(stale_work_ids)
            self._commit_changes()

            logger.info(f"Clustered record: {record}")

            self._update_elastic_search(
                work_to_index=work, works_to_delete=stale_work_ids
            )
            logger.info(f"Indexed {work} in ElasticSearch")

            return records
        except Exception as e:
            logger.exception(f"Failed to cluster record {record}")
            raise e

    def _get_clustered_work_and_records(self, record: Record):
        """Coordinates the clustering process for a single record.
        """
        # Blocking - Find candidate related records
        records = self.blocker.find_candidate_records(record)
        record_ids = [r.id for r in records]
        
        # Step 2: Clustering - Group records into edition clusters
        clustered_editions = self._cluster_records(records)
        
        # Step 3: Build FRBR model - Create Work/Edition/Item objects
        work, stale_work_ids = self._create_work_from_editions(
            clustered_editions, records
        )
        
        # Step 4: Update record status
        self._update_cluster_status(record_ids)

        return work, stale_work_ids, records

    def _commit_changes(self):
        try:
            self.db_manager.commit_changes()
        except Exception as e:
            self.db_manager.session.rollback()
            raise e

    def _update_elastic_search(self, work_to_index: Work, works_to_delete: set):
        self.elastic_search_manager.delete_work_records(works_to_delete)
        self._index_work_in_elastic_search(work_to_index)

    def _delete_stale_works(self, work_ids: set[str]):
        self.db_manager.delete_records_by_query(
            self.db_manager.session.query(Work).filter(Work.id.in_(list(work_ids)))
        )

    def _cluster_records(self, records: list[Record]):
        """Groups records into clusters using KMeans clustering.
        
        Uses KMeansManager to:
        1. Create feature vectors from record metadata
        2. Run KMeans clustering
        3. Group results by publication year
        
        Each cluster represents a work, and each group of records for a given publication year
        in that cluster is assumed to be an edition of that work.
        """
        kmean_manager = KMeansManager(records)

        kmean_manager.createDF()
        kmean_manager.generateClusters()
        editions = kmean_manager.parseEditions()

        return editions

    def _update_cluster_status(
        self, record_ids: list[str], cluster_status: bool = True
    ):
        (
            self.db_manager.session.query(Record)
            .filter(Record.id.in_(list(set(record_ids))))
            .update({"cluster_status": cluster_status, "frbr_status": "complete"})
        )

    def _create_work_from_editions(self, editions: list, records: list[Record]) -> tuple[Work, set[str]]:
        """Creates a Work object from clustered editions.
        
        Uses SFRRecordManager to:
        1. Build Work data structure from records
        2. Save Work to database
        3. Merge with any existing Works
        """
        record_manager = SFRRecordManager(
            self.db_manager.session, self.constants["iso639"]
        )

        work_data = record_manager.buildWork(records, editions)

        record_manager.saveWork(work_data)

        stale_work_ids = record_manager.mergeRecords()

        return record_manager.work, stale_work_ids

    def _index_work_in_elastic_search(self, work: Work):
        work_documents = []

        elastic_manager = SFRElasticRecordManager(work)
        elastic_manager.getCreateWork()
        work_documents.append(elastic_manager.work)

        # TODO: save single work
        self.elastic_search_manager.save_work_records(work_documents)
