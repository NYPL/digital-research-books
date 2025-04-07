import os
import requests
from requests.exceptions import ReadTimeout, HTTPError

from logger import create_log
from managers.cover_fetchers.fetcher_abc import FetcherABC

logger = create_log(__name__)


class GoogleBooksFetcher(FetcherABC):
    ORDER = 3
    SOURCE = 'googlebooks'
    GOOGLE_BOOKS_SEARCH = 'https://www.googleapis.com/books/v1/volumes?q={}:{}&key={}'
    GOOGLE_BOOKS_VOLUME = 'https://www.googleapis.com/books/v1/volumes/{}?key={}'
    IMAGE_SIZES = ['small', 'thumbnail', 'smallThumbnail']

    def __init__(self, *args):
        super().__init__(*args)

        self.apiKey = os.environ['GOOGLE_BOOKS_KEY']

        self.uri = None
        self.mediaType = None

    def hasCover(self):
        for value, source in self.identifiers:
            if source in ['isbn', 'lccn', 'oclc']:
                gbVolume = self.fetchVolume(value, source)

                if gbVolume:
                    self.coverID = '{}_{}'.format(source, value)
                    return self.fetchCover(gbVolume)

        return False

    def fetchVolume(self, value, source):
        logger.info(f'Fetching Google Books cover for {value} ({source})')

        googleSearchURI = self.GOOGLE_BOOKS_SEARCH.format(source, value, self.apiKey)

        searchResp = GoogleBooksFetcher.getAPIResponse(googleSearchURI)

        if searchResp and\
            searchResp.get('kind', '') == 'books#volumes' and\
            searchResp.get('totalItems', 0) == 1 and\
            searchResp.get('items', None) is not None:
            return searchResp['items'][0]

    def fetchCover(self, volume):
        googleVolumeURI = self.GOOGLE_BOOKS_VOLUME.format(volume['id'], self.apiKey)

        volumeResp = GoogleBooksFetcher.getAPIResponse(googleVolumeURI)

        try:
            coverLinks = volumeResp['volumeInfo']['imageLinks']
        except (KeyError, AttributeError, TypeError):
            return False

        for imageSize in self.IMAGE_SIZES:
            try:
                self.uri = coverLinks[imageSize]
                self.mediaType = 'image/jpeg'

                return True
            except KeyError:
                pass

        return False

    def downloadCoverFile(self):
        try:
            googleResponse = requests.get(self.uri, timeout=5)
            googleResponse.raise_for_status()

            return googleResponse.content
        except (ReadTimeout, HTTPError):
            pass


    @staticmethod
    def getAPIResponse(reqURI):
        try:
            apiResponse = requests.get(reqURI, timeout=5)
            apiResponse.raise_for_status()

            return apiResponse.json()
        except (ReadTimeout, HTTPError):
            pass
