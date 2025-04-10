from lxml import etree
import re

from logger import create_log
from mappings.oclc_bib import OCLCBibMapping
from mappings.oclcCatalog import CatalogMapping
from managers import DBManager, OCLCCatalogManager, RedisManager
from model import Record
from .record_buffer import RecordBuffer


logger = create_log(__name__)


class RecordFRBRizer:

    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

        self.oclc_catalog_manager = OCLCCatalogManager()
        
        self.redis_manager = RedisManager()
        self.redis_manager.create_client()

        self.record_buffer = RecordBuffer(db_manager=self.db_manager)

    def frbrize_record(self, record: Record) -> Record:
        self._add_works_for_record(record=record)

        self.record_buffer.flush()

        record.frbr_status = 'complete'

        self.db_manager.session.add(record)
        self.db_manager.session.commit()

        logger.info(f'FRBRized record: {record}')

        return record

    def _add_works_for_record(self, record: Record):
        author = record.authors[0].split('|')[0] if record.authors else None
        title = record.title

        for id, id_type in self._get_queryable_identifiers(record.identifiers):
            if self.redis_manager.check_or_set_key('classify', id, id_type):
                continue

            search_query = self.oclc_catalog_manager.generate_search_query(identifier=id, identifier_type=id_type)
            self._add_works(self.oclc_catalog_manager.query_bibs(query=search_query))

        if self.record_buffer.ingest_count == 0 and len(self.record_buffer.records) == 0 and author and title:
            search_query = self.oclc_catalog_manager.generate_search_query(author=author, title=title)
            self._add_works(self.oclc_catalog_manager.query_bibs(query=search_query))

    def _add_works(self, oclc_bibs: list):
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
