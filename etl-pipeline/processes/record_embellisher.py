import re
from typing import Optional

from logger import create_log
from mappings.oclc_bib import map_oclc_record
from managers import DBManager, OCLCCatalogManager, RedisManager
from model import Record, RecordState
from .record_buffer import RecordBuffer
import services.monitor as monitor


logger = create_log(__name__)


class RecordEmbellisher:

    def __init__(self, db_manager: DBManager, redis_manager: RedisManager):
        self.db_manager = db_manager
        self.redis_manager = redis_manager

        self.oclc_catalog_manager = OCLCCatalogManager()

        self.record_buffer = RecordBuffer(db_manager=self.db_manager)

    def embellish_record(self, record: Record) -> Record:
        work_identifiers = self._add_related_bibs(record=record)

        self.record_buffer.flush()

        # TODO: deprecate frbr_status
        record.frbr_status = "complete"
        record.state = RecordState.EMBELLISHED.value
        record.identifiers = record.identifiers + list(work_identifiers)

        self.db_manager.session.commit()
        self.db_manager.session.refresh(record)

        logger.info(f"Embellished record: {record}")

        return record

    def _add_related_bibs(self, record: Record) -> set:
        work_identifiers = set()
        author = record.authors[0].split('|')[0] if record.authors else None
        title = record.title
        fell_back_to_title_author = False
        number_of_matched_bibs = 0

        queries = []

        for id, id_type in self._get_queryable_identifiers(record.identifiers):
            if not self.redis_manager.check_or_set_key('catalog', id, id_type):
                query = self.oclc_catalog_manager.generate_identifier_query(identifier=id, identifier_type=id_type)
                queries.append(query)

        queries.append(self.oclc_catalog_manager.generate_title_author_query(author=author, title=title))

        for query in queries:
            if self._is_title_author_query(query):
                if self._fallback_to_title_author_query(author, title):
                    fell_back_to_title_author = True
                else:
                    break

            matched_bibs = self.oclc_catalog_manager.query_bibs(query=query)
            bib_work_identifiers = self._add_bibs(matched_bibs)
            
            number_of_matched_bibs += len(matched_bibs)
            work_identifiers.update(bib_work_identifiers)

        monitor.track_oclc_related_records_found(
            record=record,
            num_matches=number_of_matched_bibs,
            fell_back_to_title_author=fell_back_to_title_author
        )

        return work_identifiers

    def _add_bibs(self, oclc_bibs: list) -> set:
        return { owi_number for oclc_bib in oclc_bibs if (owi_number := self._add_bib(oclc_bib)) }

    def _add_bib(self, oclc_bib: dict) -> Optional[str]:
        owi_number = oclc_bib.get('work', {}).get('id')

        oclc_bib_record = map_oclc_record(oclc_bib=oclc_bib)

        if not oclc_bib_record:
            return None

        self.record_buffer.add(record=oclc_bib_record)

        return f'{owi_number}|owi' if owi_number is not None else None

    def _get_queryable_identifiers(self, identifiers) -> set:
        return { tuple(id.split('|', 1)) for id in identifiers if re.search(r'\|(?:isbn|issn|oclc)$', id) != None }
    
    def _fallback_to_title_author_query(self, author, title):
       return self.record_buffer.ingest_count == 0 and len(self.record_buffer.records) == 0 and author and title
    
    def _is_title_author_query(self, query):
        return 'ti:' in query and 'au:' in query 