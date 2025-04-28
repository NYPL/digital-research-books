from lxml import etree
import re

from logger import create_log
from mappings.oclc_bib import OCLCBibMapping
from mappings.oclcCatalog import CatalogMapping
from managers import DBManager, OCLCCatalogManager, RedisManager
from model import Record, RecordState
from .record_buffer import RecordBuffer
from services.monitor import track_oclc_related_records_found


logger = create_log(__name__)


class RecordEmbellisher:
    """Enriches bibliographic records with related works and editions from the OCLC catalog.
    
    This class handles the process of:
    1. Finding records based on identifiers (ISBN, ISSN, OCLC numbers, etc.)
    2. Retrieving work and edition data from OCLC
    3. Creating connections between records
    4. Managing database updates
    """

    def __init__(self, db_manager: DBManager, redis_manager: RedisManager):
        self.db_manager = db_manager
        self.redis_manager = redis_manager

        self.oclc_catalog_manager = OCLCCatalogManager()

        self.record_buffer = RecordBuffer(db_manager=self.db_manager)

    def embellish_record(self, record: Record) -> Record:
        self._add_works_for_record(record=record)

        self.record_buffer.flush()

        # TODO: deprecate frbr_status
        record.frbr_status = 'complete'
        record.state = RecordState.EMBELLISHED.value

        self.db_manager.session.commit()
        self.db_manager.session.refresh(record)

        logger.info(f'Embellished record: {record}')

        return record

    def _add_works_for_record(self, record: Record):
        """Find works related to this record based on identifiers or metadata."""
        author = record.authors[0].split('|')[0] if record.authors else None
        title = record.title
        fell_back_to_title_author = False
        num_matches = 0

        # Try identifier-based matching first
        for id, id_type in self._get_queryable_identifiers(record.identifiers):
            if self.redis_manager.check_or_set_key('classify', id, id_type):
                continue

            search_query = self.oclc_catalog_manager.generate_search_query(identifier=id, identifier_type=id_type)
            matches = self.oclc_catalog_manager.query_bibs(query=search_query)
            num_matches = len(matches)
            self._add_works(matches)
        
        # Fall back to author/title search if no results
        if self.record_buffer.ingest_count == 0 and len(self.record_buffer.records) == 0 and author and title:
            fell_back_to_title_author = True
            search_query = self.oclc_catalog_manager.generate_search_query(author=author, title=title)
            matches = self.oclc_catalog_manager.query_bibs(query=search_query)
            num_matches = len(matches)
            self._add_works(matches)

        # Track the event
        track_oclc_related_records_found(
            record=record,
            num_matches=num_matches,
            fell_back_to_title_author=fell_back_to_title_author
        )

    def _add_works(self, oclc_bibs: list):
        """Process a list of OCLC bibliographic records."""
        for oclc_bib in oclc_bibs:
            owi_number, related_oclc_numbers = self._add_work(oclc_bib) 
            
            for oclc_number, uncached in self.redis_manager.multi_check_or_set_key('catalog', related_oclc_numbers, 'oclc'):
                if not uncached:
                    continue

                self._add_edition(owi_number, oclc_number)

    def _add_work(self, oclc_bib: dict) -> tuple:
        oclc_number = oclc_bib.get('identifier', {}).get('oclcNumber')
        owi_number = oclc_bib.get('work', {}).get('id')
        related_oclc_numbers = []

        if not oclc_number or not owi_number:
            logger.warning(f'Unable to get identifiers for bib: {oclc_bib}')
            return (owi_number, related_oclc_numbers)

        if self.redis_manager.check_or_set_key('classifyWork', owi_number, 'owi'):
            return (owi_number, related_oclc_numbers)

        related_oclc_numbers = self.oclc_catalog_manager.get_related_oclc_numbers(oclc_number=oclc_number)

        oclc_bib_mapping = OCLCBibMapping(oclc_bib=oclc_bib,related_oclc_numbers=list(set(related_oclc_numbers)))
        self.record_buffer.add(record=oclc_bib_mapping.record)

        return (owi_number, related_oclc_numbers)

    def _add_edition(self, owi_number: int, oclc_number: str):
        try:
            catalog_record = self.oclc_catalog_manager.query_catalog(oclc_number)

            parsed_marc_xml = etree.fromstring(catalog_record.encode('utf-8'))
            
            catalog_record_mapping = CatalogMapping(parsed_marc_xml, { 'oclc': 'http://www.loc.gov/MARC21/slim' }, {})
            catalog_record_mapping.applyMapping()
            catalog_record_mapping.record.identifiers.append(f'{owi_number}|owi')
            
            self.record_buffer.add(catalog_record_mapping.record)
        except Exception:
            logger.exception(f'Unable to add edition with OCLC number: {oclc_number}')
            return

    def _get_queryable_identifiers(self, identifiers) -> set:
        return { tuple(id.split('|', 1)) for id in identifiers if re.search(r'\|(?:isbn|issn|oclc)$', id) != None }
