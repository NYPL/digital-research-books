import os
import requests
from requests.exceptions import Timeout, ConnectionError
from typing import Optional

from logger import createLog
from managers.oclc_auth import OCLCAuthManager


logger = createLog(__name__)


class OCLCCatalogManager:
    CATALOG_URL = 'https://metadata.api.oclc.org/worldcat/manage/bibs/{}'
    OCLC_SEARCH_URL = 'https://americas.discovery.api.oclc.org/worldcat/search/v2/'
    ITEM_TYPES = ['archv', 'audiobook', 'book', 'encyc', 'jrnl']
    LIMIT = 50
    MAX_NUMBER_OF_RECORDS = 100
    BEST_MATCH = 'bestMatch'

    def __init__(self):
        self.oclc_key = os.environ['OCLC_API_KEY']

    def query_catalog(self, oclc_no):
        catalog_query = self.CATALOG_URL.format(oclc_no)

        for _ in range(0, 3):
            try:
                token = OCLCAuthManager.get_metadata_token()
                headers = { 'Authorization': f'Bearer {token}' }
                catalog_response = requests.get(catalog_query, headers=headers, timeout=3)

                if catalog_response.status_code != 200:
                    logger.warning(f'OCLC catalog request failed with status {catalog_response.status_code}')
                    return None

                return catalog_response.text
            except (Timeout, ConnectionError):
                logger.warning(f'Could not connect to {catalog_query} or timed out')
            except Exception as e:
                logger.error(f'Failed to query catalog with query {catalog_query} due to {e}')
                return None
            
        return None

    def get_related_oclc_numbers(self, oclc_number: int) -> list[int]:
        related_oclc_numbers = []

        try:
            other_editions_response = self._get_other_editions(oclc_number=oclc_number, offset=0)

            if not other_editions_response:
                return related_oclc_numbers
            
            number_of_related_bibs = other_editions_response['numberOfRecords']
            
            if number_of_related_bibs <= self.LIMIT:
                related_oclc_bibs = other_editions_response['briefRecords']

                return self._get_oclc_number_from_bibs(oclc_number=oclc_number, oclc_bibs=related_oclc_bibs)
            
            offset = self.LIMIT
            while offset <= min(number_of_related_bibs, self.MAX_NUMBER_OF_RECORDS):
                other_editions_response = self._get_other_editions(oclc_number=oclc_number, offset=offset)

                if not other_editions_response:
                    continue

                related_oclc_bibs = other_editions_response['briefRecords']

                related_oclc_numbers.extend(
                    self._get_oclc_number_from_bibs(oclc_number=oclc_number, oclc_bibs=related_oclc_bibs)
                )
                offset += self.LIMIT

            return related_oclc_numbers
        except Exception as e:
            logger.error(f'Failed to get related OCLC numbers for {oclc_number} due to {e}')
            return related_oclc_numbers

    def _get_other_editions(self, oclc_number: int, offset: int=0):
        other_editions_url = f'https://americas.discovery.api.oclc.org/worldcat/search/v2/brief-bibs/{oclc_number}/other-editions'

        try:
            token = OCLCAuthManager.get_search_token()
            headers = { 'Authorization': f'Bearer {token}' }

            other_editions_response = requests.get(
                other_editions_url,
                headers=headers,
                params={
                    'offset': offset or None,
                    'limit': self.LIMIT,
                    'orderBy': self.BEST_MATCH,
                    'itemTypes': self.ITEM_TYPES
                }
            )

            status_code = other_editions_response.status_code

            if other_editions_response.status_code != 200:
                logger.warning(
                    f'OCLC other editions request for OCLC number {oclc_number} failed with status: {status_code} '
                    f'due to: {self._get_error_detail(other_editions_response)}'
                )

                return None

            return other_editions_response.json()
        except Exception as e:
            logger.error(f'Failed to query other editions endpoint {other_editions_url} due to {e}')
            return None

    def _get_oclc_number_from_bibs(self, oclc_number: int, oclc_bibs) -> int:
        return [int(edition['oclcNumber']) for edition in oclc_bibs if int(edition['oclcNumber']) != oclc_number]

    def query_bibs(self, query: str):
        bibs = []

        try:
            bibs_response = self._search_bibs(query=query, offset=0)

            if not bibs_response:
                return bibs
            
            number_of_bibs = bibs_response['numberOfRecords']
            
            if number_of_bibs <= self.LIMIT:
                return bibs_response.get('bibRecords', [])            
            
            offset = self.LIMIT
            while offset <= min(number_of_bibs, self.MAX_NUMBER_OF_RECORDS):
                bibs_response = self._search_bibs(query=query, offset=offset)

                if not bibs_response:
                    continue

                bibs.extend(bibs_response.get('bibRecords', []))
                offset += self.LIMIT

            return bibs
        except Exception as e:
            logger.error(f'Failed to query search bibs with query {query} due to {e}')
            return bibs

    def _search_bibs(self, query: str, offset: int=0):
        try:
            token = OCLCAuthManager.get_search_token()
            bibs_endpoint = self.OCLC_SEARCH_URL + 'bibs'
            headers = { "Authorization": f"Bearer {token}" }

            bibs_response = requests.get(
                bibs_endpoint,
                headers=headers,
                params={
                    'q': query,
                    'offset': offset or None,
                    'limit': self.LIMIT,
                    'orderBy': self.BEST_MATCH,
                    'itemType': self.ITEM_TYPES
                }
            )

            status_code = bibs_response.status_code

            if status_code != 200:
                logger.warning(
                    f'OCLC search bibs request for query {query} failed with status: {status_code} '
                    f'due to: {self._get_error_detail(bibs_response)}'
                )
                
                return None
            
            return bibs_response.json()
        except Exception as e:
            logger.error(f'Failed to query {bibs_endpoint} with query {query} due to {e}')
            return None

    def generate_search_query(self, identifier=None, identifier_type=None, title=None, author=None):
        if identifier and identifier_type:
            return self._generate_identifier_query(identifier, identifier_type)
        elif title and author:
            return self._generate_title_author_query(title, author)
        else:
            raise OCLCCatalogError('Record lacks identifier or title/author pair')
    
    def _generate_identifier_query(self, identifier, identifier_type):
        identifier_map = { 
            "isbn": "bn",
            "issn": "in",
            "oclc": "no"
        }

        return f"{identifier_map[identifier_type]}: {identifier}"

    def _generate_title_author_query(self, title, author):
        return f"ti:{title} au:{author}"
    
    def _get_error_detail(oclc_response) -> Optional[str]:
        default_error_detail = 'unknown'

        try:
            return oclc_response.json().get('detail', default_error_detail)
        except Exception:
            return default_error_detail

class OCLCCatalogError(Exception):
    def __init__(self, message=None):
        self.message = message
