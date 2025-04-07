import hashlib
import os
import requests
from requests.exceptions import ReadTimeout

from logger import create_log
from managers.cover_fetchers.abstractFetcher import AbstractFetcher

logger = create_log(__name__)


class ContentCafeFetcher(AbstractFetcher):
    ORDER = 4
    SOURCE = 'contentcafe'
    NO_COVER_HASH = '7ba0a6a15b5c1d346719a6d079e850a3'
    CONTENT_CAFE_URL = 'http://contentcafe2.btol.com/ContentCafe/Jacket.aspx?userID={}&password={}&type=L&Value={}'

    def __init__(self, *args):
        super().__init__(*args)

        self.apiUser = os.environ['CONTENT_CAFE_USER']
        self.apiPswd = os.environ['CONTENT_CAFE_PSWD']

        self.uri = None
        self.content = None
        self.mediaType = None

    def hasCover(self):
        for value, source in self.identifiers:
            if source != 'isbn': continue

            if self.fetchVolumeCover(value):
                self.coverID = value
                return True

        return False

    def fetchVolumeCover(self, isbn):
        logger.info(f'Fetching contentcafe cover for {isbn}')

        jacketURL = self.CONTENT_CAFE_URL.format(self.apiUser, self.apiPswd, isbn)

        try:
            ccResponse = requests.get(jacketURL, timeout=5, stream=True)
        except ReadTimeout:
            return False

        if ccResponse.status_code == 200:
            imageStartChunk = ccResponse.raw.read(1024)
            if self.isNoCoverImage(imageStartChunk) is False:
                self.content = imageStartChunk + ccResponse.raw.data
                return True

        return False

    def isNoCoverImage(self, rawBytes):
        return hashlib.md5(rawBytes).hexdigest() == self.NO_COVER_HASH

    def downloadCoverFile(self):
        return self.content