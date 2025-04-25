import re
from pottery import Redlock
from sqlalchemy.exc import DataError
from time import sleep

from logger import create_log
from managers import (
    DBManager,
    ElasticsearchManager,
    KMeansManager,
    SFRElasticRecordManager,
    SFRRecordManager,
    RedisManager
)
from constants.get_constants import get_constants
from model import Record, Work, RecordState


logger = create_log(__name__)


class RecordClusterer:
    """Clusters related bibliographic records and transforms them into FRBR model objects.
    
    This class handles the process of:
    1. Finding related records through identifier and title matching
    2. Clustering those records using machine learning
    3. Creating Work/Edition/Item objects from the clusters
    4. Managing database updates and search indexing
    """
    # Maximum number of "hops" to follow when matching records through identifiers
    MAX_MATCH_DISTANCE = 4
    # Maximum number of records that can be in a cluster
    CLUSTER_SIZE_LIMIT = 10000
    # Regular expression to identify identifiers that should be matched
    IDENTIFIERS_TO_MATCH = r"\|(?:isbn|issn|oclc|lccn|owi)$"
    CLUSTER_TIMEOUT = 60 * 60 # 1 hour
    CLUSTER_LOCK_KEY_PREFIX = 'cluster_lock_'

    def __init__(self, db_manager: DBManager, redis_manager: RedisManager):
        self.db_manager = db_manager

        self.elastic_search_manager = ElasticsearchManager()

        self.elastic_search_manager.create_elastic_connection()
        self.elastic_search_manager.create_elastic_search_ingest_pipeline()
        self.elastic_search_manager.create_elastic_search_index()

        self.redis_manager = redis_manager

        self.constants = get_constants()

    def cluster_record(self, record) -> list[Record]:
        """Clusters a single record and updates the database and Elasticsearch.
        """
        try:
            record_lock = Redlock(key=f'{self.CLUSTER_LOCK_KEY_PREFIX}{record.id}', masters={self.redis_manager.client}, auto_release_time=self.CLUSTER_TIMEOUT)

            with record_lock:
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
        except ConcurrentClusterException:
            logger.info(f"Skipping step to cluster record: {record}")
            return []
        except Exception as e:
            logger.exception(f"Failed to cluster record {record}")
            raise e

    def _get_clustered_work_and_records(self, record: Record):
        """Coordinates the clustering process for a single record.
        
        1. Finds all "matches" for the record by matching identifiers and partially comparing titles
        2. Clusters the matched records using kMeans
        3. Creates editions from the clusers
        4. Creates a Work object from the editions
        5. Updates record status in the database
        """
        matched_record_ids = self._find_all_matching_records(record) + [record.id]

        clustered_editions, records = self._cluster_matched_records(matched_record_ids)
        work, stale_work_ids = self._create_work_from_editions(
            clustered_editions, records
        )
        self._update_cluster_status(matched_record_ids)

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

    def _cluster_matched_records(self, record_ids: list[str]):
        """Groups matched records into clusters using KMeans clustering.
        
        Uses KMeansManager to:
        1. Create feature vectors from record metadata
        2. Run KMeans clustering
        3. Group results by publication year
        Each cluster represents a work, and each group of records for a given publication year in that cluster
        is assumed to be an edition of that work.
        """
        records = (
            self.db_manager.session.query(Record)
            .filter(Record.id.in_(record_ids))
            .all()
        )

        kmean_manager = KMeansManager(records)

        kmean_manager.createDF()
        kmean_manager.generateClusters()
        editions = kmean_manager.parseEditions()

        return editions, records

    def _update_cluster_status(
        self, record_ids: list[str], cluster_status: bool = True
    ):
        (
            self.db_manager.session.query(Record)
            .filter(Record.id.in_(list(set(record_ids))))
            .update({"cluster_status": cluster_status, "frbr_status": "complete", "state": RecordState.CLUSTERED.value })
        )

    def _find_all_matching_records(self, record: Record) -> list[str]:
        """Finds all records that might be related to the input record.
        
        Uses an iterative process to find matches:
        1. Start with record's identifiers
        2. Find records matching one of the identifiers (records have multiple identifiers)
        3. For each match, check title similarity
        4. If similar, add their identifiers to check
        5. Repeat up to MAX_MATCH_DISTANCE times
        """
        tokenized_record_title = self._tokenize_title(record.title)
        ids_to_check = {
            id for id in record.identifiers if re.search(self.IDENTIFIERS_TO_MATCH, id)
        }

        matched_record_ids = { record.id }
        checked_ids = set()

        for match_distance in range(0, self.MAX_MATCH_DISTANCE):
            matched_records = self._get_matched_records(
                list(ids_to_check), matched_record_ids.copy()
            )

            if not matched_records:
                break

            checked_ids.update(ids_to_check)
            ids_to_check.clear()

            for (
                matched_record_title,
                matched_record_id,
                matched_record_identifiers,
                *_,
            ) in matched_records:
                if not matched_record_title:
                    logger.warning("Invalid title found in matched records")
                    continue

                tokenized_matched_record_title = self._tokenize_title(
                    matched_record_title
                )

                if match_distance > 0 and not self._titles_overlap(
                    tokenized_record_title, tokenized_matched_record_title
                ):
                    continue

                ids_to_check.update(
                    {
                        id
                        for id in matched_record_identifiers
                        if re.search(self.IDENTIFIERS_TO_MATCH, id)
                        and id not in checked_ids
                    }
                )
                matched_record_ids.add(matched_record_id)

        if len(matched_record_ids) > self.CLUSTER_SIZE_LIMIT:
            raise Exception(
                f"Records matched is greater than {self.CLUSTER_SIZE_LIMIT}"
            )

        return list(matched_record_ids)

    def _get_matched_records(
        self, identifiers: list[str], already_matched_record_ids: list[str]
    ):
        """Queries database for records matching the given identifiers to generate 
        a candidate pool of potentially related records.
        
        Processes identifiers in batches to avoid database query size limits.
        
        Returns:
            list[tuple]: List of (title, id, identifiers, has_part) tuples for matching records
        """
        batch_size = 100
        matched_records = []

        for i in range(0, len(identifiers), batch_size):
            id_batch = self._format_identifiers(identifiers[i : i + batch_size])

            try:
                records = (
                    self.db_manager.session.query(
                        Record.title, Record.id, Record.identifiers, Record.has_part
                    )
                    .filter(~Record.id.in_(already_matched_record_ids))
                    .filter(Record.identifiers.overlap(id_batch))
                    .filter(Record.title.isnot(None))
                    .all()
                )

                cluster_lock_keys = [f'{self.CLUSTER_LOCK_KEY_PREFIX}{record[1]}' for record in records]

                if self.redis_manager.any_locked(cluster_lock_keys):
                    raise ConcurrentClusterException('Currently clustering group of records')

                matched_records.extend(records)
            except DataError:
                logger.exception("Unable to get matching records")

        return matched_records

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

    def _titles_overlap(
        self, tokenized_record_title: set, tokenized_matched_record_title: set
    ):
        """Determines if two titles are similar enough to be considered matching.
        
        Rules:
        1. Single word titles must be subset/superset
        2. Multi-word titles must share at least 2 words
        """

        if (
            len(tokenized_record_title) == 1
            and not tokenized_record_title <= tokenized_matched_record_title
        ):
            return False
        elif (
            len(tokenized_matched_record_title) == 1
            and not tokenized_record_title >= tokenized_matched_record_title
        ):
            return False
        elif (
            len(tokenized_record_title) > 1 and len(tokenized_matched_record_title) > 1
        ) and len(tokenized_record_title & tokenized_matched_record_title) < 2:
            return False

        return True

    def _tokenize_title(self, title: str) -> set[str]:
        """Converts a title string into a set of normalized "tokens" (words).
        
        1. Extracts words using regex
        2. Converts to lowercase
        3. Removes common stop words
        """
        title_tokens = re.findall(r"(\w+)", title.lower())

        return set(title_tokens) - set(["a", "an", "the", "of"])

    def _format_identifiers(self, identifiers: list[str]):
        """Formats identifiers for PostgreSQL array overlap query.
        Handles escaping of special characters and creates proper array syntax.
        """
        formatted_ids = []

        for id in identifiers:
            formatted_id = f'"{id}"' if re.search(r"[{},]{1}", id) else id
            formatted_ids.append(formatted_id)

        return "{{{}}}".format(",".join(formatted_ids))
    

class ConcurrentClusterException(Exception):

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
