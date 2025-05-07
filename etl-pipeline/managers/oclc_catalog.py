import os
import requests
from requests.exceptions import Timeout, ConnectionError
from typing import Optional

from logger import create_log
from managers.oclc_auth import OCLCAuthManager


logger = create_log(__name__)


class OCLCCatalogManager:
    METADATA_BIB_URL = "https://metadata.api.oclc.org/worldcat/manage/bibs/{}"
    OCLC_SEARCH_URL = "https://americas.discovery.api.oclc.org/worldcat/search/v2/"
    ITEM_SUB_TYPE = ["book-digital"]
    LIMIT = 50
    MAX_NUMBER_OF_RECORDS = 100
    BEST_MATCH = "bestMatch"

    def __init__(self):
        self.rate_limited = False

    def query_catalog(self, oclc_no):
        catalog_query = self.METADATA_BIB_URL.format(oclc_no)

        for _ in range(0, 3):
            try:
                token = OCLCAuthManager.get_metadata_token()
                headers = {"Authorization": f"Bearer {token}"}

                catalog_response = requests.get(
                    catalog_query, headers=headers, timeout=5
                )

                if catalog_response.status_code != 200:
                    logger.warning(
                        f"OCLC catalog request failed with status {catalog_response.status_code}"
                    )
                    return None

                return catalog_response.text
            except (Timeout, ConnectionError):
                logger.warning(f"Could not connect to {catalog_query} or timed out")
            except Exception as e:
                logger.error(
                    f"Failed to query catalog with query {catalog_query} due to {e}"
                )
                return None

        return None

    def get_related_oclc_numbers(self, oclc_number: int) -> list[int]:
        related_oclc_numbers = []

        try:
            other_editions_response = self._get_other_editions(
                oclc_number=oclc_number, offset=0
            )

            if not other_editions_response:
                return related_oclc_numbers

            number_of_related_bibs = other_editions_response.get("numberOfRecords", 0)

            if number_of_related_bibs <= self.LIMIT:
                related_oclc_bibs = other_editions_response.get("briefRecords", None)

                if related_oclc_bibs is None:
                    return related_oclc_numbers

                return self._get_oclc_number_from_bibs(
                    oclc_number=oclc_number, oclc_bibs=related_oclc_bibs
                )

            offset = self.LIMIT
            while offset <= min(number_of_related_bibs, self.MAX_NUMBER_OF_RECORDS):
                other_editions_response = self._get_other_editions(
                    oclc_number=oclc_number, offset=offset
                )

                if not other_editions_response:
                    continue

                related_oclc_bibs = other_editions_response.get("briefRecords", None)

                if related_oclc_bibs is None:
                    return related_oclc_numbers

                related_oclc_numbers.extend(
                    self._get_oclc_number_from_bibs(
                        oclc_number=oclc_number, oclc_bibs=related_oclc_bibs
                    )
                )
                offset += self.LIMIT

            return related_oclc_numbers
        except Exception as e:
            logger.error(
                f"Failed to get related OCLC numbers for {oclc_number} due to {e}"
            )
            return related_oclc_numbers

    def _get_other_editions(self, oclc_number: int, offset: int = 0):
        other_editions_url = f"https://americas.discovery.api.oclc.org/worldcat/search/v2/brief-bibs/{oclc_number}/other-editions"

        try:
            token = OCLCAuthManager.get_search_token()
            headers = {"Authorization": f"Bearer {token}"}

            other_editions_response = requests.get(
                other_editions_url,
                headers=headers,
                params={
                    "offset": offset or None,
                    "limit": self.LIMIT,
                    "orderBy": self.BEST_MATCH,
                    "itemSubType": self.ITEM_SUB_TYPE,
                },
            )

            if not other_editions_response.ok:
                logger.warning(
                    f"OCLC other editions request for OCLC number {oclc_number} failed with status: {other_editions_response.status_code} "
                    f"due to: {self._get_error_detail(other_editions_response)}"
                )

                if other_editions_response.status_code == 429:
                    self.rate_limited = True

                return None

            return other_editions_response.json()
        except Exception as e:
            logger.error(
                f"Failed to query other editions endpoint {other_editions_url} due to {e}"
            )
            return None

    def _get_oclc_number_from_bibs(self, oclc_number: int, oclc_bibs) -> int:
        return [
            int(edition["oclcNumber"])
            for edition in oclc_bibs
            if int(edition["oclcNumber"]) != oclc_number
        ]

    def query_bibs(self, query: str):
        bibs = []

        try:
            bibs_response = self._search_bibs(query=query, offset=0)

            if not bibs_response:
                return bibs

            number_of_bibs = bibs_response["numberOfRecords"]

            if number_of_bibs <= self.LIMIT:
                return bibs_response.get("bibRecords", [])

            offset = self.LIMIT
            while offset <= min(number_of_bibs, self.MAX_NUMBER_OF_RECORDS):
                bibs_response = self._search_bibs(query=query, offset=offset)

                if not bibs_response:
                    continue

                bibs.extend(bibs_response.get("bibRecords", []))
                offset += self.LIMIT

            return bibs
        except Exception as e:
            logger.error(f"Failed to query search bibs with query {query} due to {e}")
            return bibs

    def _search_bibs(self, query: str, offset: int = 0):
        try:
            token = OCLCAuthManager.get_search_token()
            bibs_endpoint = self.OCLC_SEARCH_URL + "bibs"
            headers = {"Authorization": f"Bearer {token}"}

            bibs_response = requests.get(
                bibs_endpoint,
                headers=headers,
                params={
                    "q": query,
                    "offset": offset or None,
                    "limit": self.LIMIT,
                    "orderBy": self.BEST_MATCH,
                    "itemSubType": self.ITEM_SUB_TYPE,
                },
            )

            if not bibs_response.ok:
                logger.warning(
                    f"OCLC search bibs request for query {query} failed with status: {bibs_response.status_code} "
                    f"due to: {self._get_error_detail(bibs_response)}"
                )

                if bibs_response.status_code == 429:
                    self.rate_limited = True

                return None

            return bibs_response.json()
        except Exception as e:
            logger.error(
                f"Failed to query {bibs_endpoint} with query {query} due to {e}"
            )
            return None
    
    def generate_identifier_query(self, identifier, identifier_type):
        identifier_map = { 
            "isbn": "bn",
            "issn": "in",
            "oclc": "no"
        }

        return f"{identifier_map[identifier_type]}: {identifier}"

    def generate_title_author_query(self, title, author):
        return f"ti:{title} au:{author}"

    def _get_error_detail(self, oclc_response) -> Optional[str]:
        default_error_detail = "unknown"

        try:
            return oclc_response.json().get("detail", default_error_detail)
        except Exception:
            return default_error_detail


class OCLCCatalogError(Exception):
    def __init__(self, message=None):
        self.message = message
