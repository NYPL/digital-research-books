import hashlib
import os
import requests
from requests.exceptions import ReadTimeout

from logger import create_log
from managers.cover_fetchers.fetcher_abc import FetcherABC

logger = create_log(__name__)


class ContentCafeFetcher(FetcherABC):
    ORDER = 4
    SOURCE = "contentcafe"
    NO_COVER_HASH = "7ba0a6a15b5c1d346719a6d079e850a3"
    CONTENT_CAFE_URL = "http://contentcafe2.btol.com/ContentCafe/Jacket.aspx?userID={}&password={}&type=L&Value={}"

    def __init__(self, *args):
        super().__init__(*args)

        self.api_user = os.environ["CONTENT_CAFE_USER"]
        self.api_pswd = os.environ["CONTENT_CAFE_PSWD"]

        self.uri = None
        self.content = None
        self.media_type = None

    def has_cover(self):
        for value, source in self.identifiers:
            if source != "isbn":
                continue

            if self.fetch_volume_cover(value):
                self.coverID = value
                return True

        return False

    def fetch_volume_cover(self, isbn):
        logger.info(f"Fetching contentcafe cover for {isbn}")

        jacket_url = self.CONTENT_CAFE_URL.format(self.api_user, self.api_pswd, isbn)

        try:
            response = requests.get(jacket_url, timeout=5, stream=True)
        except ReadTimeout:
            return False

        if response.status_code == 200:
            image_start_chunk = response.raw.read(1024)
            if self.is_no_cover_image(image_start_chunk) is False:
                self.content = image_start_chunk + response.raw.data
                return True

        return False

    def is_no_cover_image(self, rawBytes):
        return hashlib.md5(rawBytes).hexdigest() == self.NO_COVER_HASH

    def download_cover_file(self):
        return self.content
