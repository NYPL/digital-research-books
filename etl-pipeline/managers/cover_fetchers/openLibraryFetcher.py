import requests
from requests.exceptions import ReadTimeout, HTTPError

from logger import create_log
from managers.cover_fetchers.fetcher_abc import FetcherABC
from model import OpenLibraryCover

logger = create_log(__name__)


class OpenLibraryFetcher(FetcherABC):
    ORDER = 2
    SOURCE = 'openlibrary'
    OL_COVER_URL = 'http://covers.openlibrary.org/b/id/{}-L.jpg'

    def __init__(self, *args):
        super().__init__(*args)

        self.session = args[1]

        self.uri = None
        self.mediaType = None

    def hasCover(self):
        for value, source in self.identifiers:
            if source in ['isbn', 'lccn', 'oclc']:
                coverStatus = self.fetchVolumeCover(value, source)

                if coverStatus is True:
                    self.coverID = '{}_{}'.format(source, value)
                    return True

        return False

    def fetchVolumeCover(self, value, source):
        logger.info(f'Fetching openLibrary cover for {value} ({source})')

        coverRow = self.session.query(OpenLibraryCover)\
            .filter(OpenLibraryCover.name == source)\
            .filter(OpenLibraryCover.value == value)\
            .first()

        if coverRow:
            self.setCoverPageURL(coverRow.cover_id)
            return True

        return False

    def setCoverPageURL(self, coverID):
        self.uri = self.OL_COVER_URL.format(coverID)
        self.mediaType = 'image/jpeg'

    def downloadCoverFile(self):
        try:
            olResponse = requests.get(self.uri, timeout=5)
            olResponse.raise_for_status()

            return olResponse.content
        except (ReadTimeout, HTTPError):
            pass
