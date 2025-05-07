import requests
from requests.exceptions import ReadTimeout, HTTPError

from logger import create_log
from managers.cover_fetchers.fetcher_abc import FetcherABC
from model import OpenLibraryCover

logger = create_log(__name__)


class OpenLibraryFetcher(FetcherABC):
    ORDER = 2
    SOURCE = "openlibrary"
    OL_COVER_URL = "http://covers.openlibrary.org/b/id/{}-L.jpg"

    def __init__(self, *args):
        super().__init__(*args)

        self.session = args[1]

        self.uri = None
        self.media_type = None

    def has_cover(self):
        for value, source in self.identifiers:
            if source in ["isbn", "lccn", "oclc"]:
                cover_status = self.fetch_volume_cover(value, source)

                if cover_status is True:
                    self.cover_id = "{}_{}".format(source, value)
                    return True

        return False

    def fetch_volume_cover(self, value, source):
        logger.info(f"Fetching Open Library cover for {value} ({source})")

        cover_row = (
            self.session.query(OpenLibraryCover)
            .filter(OpenLibraryCover.name == source)
            .filter(OpenLibraryCover.value == value)
            .first()
        )

        if cover_row:
            self.set_cover_page_url(cover_row.cover_id)
            return True

        return False

    def set_cover_page_url(self, coverID):
        self.uri = self.OL_COVER_URL.format(coverID)
        self.media_type = "image/jpeg"

    def download_cover_file(self):
        try:
            response = requests.get(self.uri, timeout=5)
            response.raise_for_status()

            return response.content
        except (ReadTimeout, HTTPError):
            pass
