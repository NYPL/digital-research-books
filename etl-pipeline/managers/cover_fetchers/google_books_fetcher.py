import os
import requests
from requests.exceptions import ReadTimeout, HTTPError

from logger import create_log
from managers.cover_fetchers.fetcher_abc import FetcherABC

logger = create_log(__name__)


class GoogleBooksFetcher(FetcherABC):
    ORDER = 3
    SOURCE = "googlebooks"
    GOOGLE_BOOKS_SEARCH = "https://www.googleapis.com/books/v1/volumes?q={}:{}&key={}"
    GOOGLE_BOOKS_VOLUME = "https://www.googleapis.com/books/v1/volumes/{}?key={}"
    IMAGE_SIZES = ["small", "thumbnail", "smallThumbnail"]

    def __init__(self, *args):
        super().__init__(*args)

        self.api_key = os.environ["GOOGLE_BOOKS_KEY"]

        self.uri = None
        self.media_type = None

    def has_cover(self):
        for value, source in self.identifiers:
            if source in ["isbn", "lccn", "oclc"]:
                gb_volume = self.fetch_volume(value, source)

                if gb_volume:
                    self.cover_id = "{}_{}".format(source, value)
                    return self.fetch_cover(gb_volume)

        return False

    def fetch_volume(self, value, source):
        logger.info(f"Fetching Google Books cover for {value} ({source})")

        google_search_uri = self.GOOGLE_BOOKS_SEARCH.format(source, value, self.api_key)

        response = GoogleBooksFetcher.get_api_response(google_search_uri)

        if (
            response
            and response.get("kind", "") == "books#volumes"
            and response.get("totalItems", 0) == 1
            and response.get("items", None) is not None
        ):
            return response["items"][0]

    def fetch_cover(self, volume):
        google_volume_uri = self.GOOGLE_BOOKS_VOLUME.format(volume["id"], self.api_key)

        response = GoogleBooksFetcher.get_api_response(google_volume_uri)

        try:
            cover_links = response["volumeInfo"]["imageLinks"]
        except (KeyError, AttributeError, TypeError):
            return False

        for image_size in self.IMAGE_SIZES:
            try:
                self.uri = cover_links[image_size]
                self.media_type = "image/jpeg"

                return True
            except KeyError:
                pass

        return False

    def download_cover_file(self):
        try:
            response = requests.get(self.uri, timeout=5)
            response.raise_for_status()

            return response.content
        except (ReadTimeout, HTTPError):
            pass

    @staticmethod
    def get_api_response(req_uri):
        try:
            response = requests.get(req_uri, timeout=5)
            response.raise_for_status()

            return response.json()
        except (ReadTimeout, HTTPError):
            pass
