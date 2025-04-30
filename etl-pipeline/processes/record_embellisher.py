import re
from typing import Optional

from logger import create_log
from mappings.oclc_bib import map_oclc_record
from managers import DBManager, OCLCCatalogManager, RedisManager
from model import Record, RecordState
from .record_buffer import RecordBuffer


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
        record.frbr_status = 'complete'
        record.state = RecordState.EMBELLISHED.value
        record.identifiers.extend(work_identifiers)

        self.db_manager.session.commit()
        self.db_manager.session.refresh(record)

        logger.info(f'Embellished record: {record}')

        return record

    def _add_related_bibs(self, record: Record) -> set:
        work_identifiers = set()
        author = record.authors[0].split('|')[0] if record.authors else None
        title = record.title

        for id, id_type in self._get_queryable_identifiers(record.identifiers):
            if self.redis_manager.check_or_set_key('catalog', id, id_type):
                continue

            search_query = self.oclc_catalog_manager.generate_identifier_query(identifier=id, identifier_type=id_type)
            bib_work_identifiers = self._add_bibs(self.oclc_catalog_manager.query_bibs(query=search_query))
            work_identifiers.update(bib_work_identifiers)
        
        if self._fallback_to_title_author_query(self, author, title):
            search_query = self.oclc_catalog_manager.generate_title_author_query(author=author, title=title)
            bib_work_identifiers = self._add_bibs(self.oclc_catalog_manager.query_bibs(query=search_query))
            work_identifiers.update(bib_work_identifiers)

        return work_identifiers

    def _add_bibs(self, oclc_bibs: list) -> set:
        return { owi_number := self._add_bib(oclc_bib) for oclc_bib in oclc_bibs if owi_number is not None }

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
