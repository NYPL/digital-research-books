from collections import defaultdict
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
        self.redis_manager.createRedisClient()

        self.record_buffer = RecordBuffer(db_manager=self.db_manager)

    def frbrize_record(self, record: Record) -> Record:
        owi_to_oclc_numbers = self._add_works_for_record(record=record)
        self._add_edition_records(owi_to_oclc_numbers=owi_to_oclc_numbers)

        self.record_buffer.flush()

        record.frbr_status = 'complete'

        self.db_manager.session.add(record)
        self.db_manager.session.commit()

        logger.info(f'FRBRized record: {record}')

        return record

    def _add_works_for_record(self, record: Record) -> defaultdict:
        queryable_ids = self._get_queryable_identifiers(record.identifiers)
        author = record.authors[0].split('|')[0] if record.authors else None
        title = record.title

        owi_to_oclc_numbers = defaultdict(set)

        for id, id_type in queryable_ids:
            if self.redis_manager.checkSetRedis('classify', id, id_type):
                continue

            search_query = self.oclc_catalog_manager.generate_search_query(identifier=id, identifier_type=id_type)
            owi_to_oclc_numbers.update(self._add_works(self.oclc_catalog_manager.query_bibs(query=search_query)))

        if len(owi_to_oclc_numbers) == 0 and author and title:
            search_query = self.oclc_catalog_manager.generate_search_query(author=author, title=title)
            owi_to_oclc_numbers.update(self._add_works(self.oclc_catalog_manager.query_bibs(query=search_query)))

        return owi_to_oclc_numbers

    def _add_works(self, oclc_bibs: list) -> set:
        owi_to_oclc_numbers = defaultdict(set)

        for oclc_bib in oclc_bibs:
            oclc_number = oclc_bib.get('identifier', {}).get('oclcNumber')
            owi_number = oclc_bib.get('work', {}).get('id')

            if not oclc_number or not owi_number:
                logger.warning(f'Unable to get identifiers for bib: {oclc_bib}')
                continue

            if self.redis_manager.checkSetRedis('classifyWork', owi_number, 'owi'):
                continue

            related_oclc_numbers = self.oclc_catalog_manager.get_related_oclc_numbers(oclc_number=oclc_number)

            oclc_bib_mapping = OCLCBibMapping(
                oclc_bib=oclc_bib,
                related_oclc_numbers=list(set(related_oclc_numbers))
            )

            self.record_buffer.add(record=oclc_bib_mapping.record)
            owi_to_oclc_numbers[owi_number] = set(related_oclc_numbers)

        return owi_to_oclc_numbers

    def _add_edition_records(self, owi_to_oclc_numbers: defaultdict[set]):
        for owi_number, oclc_numbers in owi_to_oclc_numbers.items():
            cached_oclc_numbers = self.redis_manager.multiCheckSetRedis('catalog', list(oclc_numbers), 'oclc')

            for oclc_number, uncached in cached_oclc_numbers:
                if not uncached:
                    continue
                
                self._add_edition_record(oclc_number=oclc_number, owi_number=owi_number)

    def _add_edition_record(self, oclc_number: int, owi_number: str):
        catalog_record = self.oclc_catalog_manager.query_catalog(oclc_number)

        try:
            parsed_marc_xml = etree.fromstring(catalog_record.encode('utf-8'))
        except Exception:
            logger.exception(f'Unable to parse OCLC catalog MARC XML for OCLC number: {oclc_number}')
            return

        catalog_record_mapping = CatalogMapping(parsed_marc_xml, { 'oclc': 'http://www.loc.gov/MARC21/slim' }, {})

        try:
            catalog_record_mapping.applyMapping()
            catalog_record_mapping.record.identifiers.append('{}|owi'.format(owi_number))
            
            self.record_buffer.add(catalog_record_mapping.record)
        except Exception:
            logger.exception(f'Unable to map OCLC catalog record for OCLC number: {oclc_number}')
            return

    def _get_queryable_identifiers(self, identifiers) -> set:
        return set([(id.split('|')[0], id.split('|')[1]) for id in identifiers if re.search(r'\|(?:isbn|issn|oclc)$', id) != None])
