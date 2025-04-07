import os
import requests
from requests.exceptions import ReadTimeout, HTTPError
from requests_oauthlib import OAuth1

from managers.cover_fetchers.fetcher_abc import FetcherABC
from logger import create_log

logger = create_log(__name__)


class HathiFetcher(FetcherABC):
    ORDER = 1
    SOURCE = 'hathi'

    def __init__(self, *args):
        super().__init__(*args)

        self.apiRoot = 'https://babel.hathitrust.org/cgi/htd'
        self.apiKey = os.environ['HATHI_API_KEY']
        self.apiSecret = os.environ['HATHI_API_SECRET']

        self.uri = None
        self.mediaType = None

    def hasCover(self):
        for value, source in self.identifiers:
            if source != 'hathi' or '.' not in value: continue

            try:
                self.fetchVolumeCover(value)

                self.coverID = value

                return True
            except HathiCoverError as e:
                logger.error('Unable to parse HathiTrust volume for cover')
                logger.debug(e.message)

        return False

    def fetchVolumeCover(self, htid):
        logger.info(f'Fetching hathi cover for {htid}')

        metsURL = '{}/structure/{}?format=json&v=2'.format(self.apiRoot, htid)

        metsResponse = self.makeHathiReq(metsURL)

        if not metsResponse:
            raise HathiCoverError('Invalid htid {}'.format(htid))

        metsJSON = metsResponse.json()

        try:
            pageList = metsJSON['METS:structMap']['METS:div']['METS:div'][:25]
        except (TypeError, KeyError):
            logger.debug(metsJSON)
            raise HathiCoverError('Unexpected METS format in hathi rec {}'.format(htid))

        rankedMETSPages = sorted(
            [HathiPage(page) for page in pageList], key=lambda x: x.pageScore, reverse=True
        )

        self.setCoverPageURL(htid, rankedMETSPages[0].pageNumber)

    def setCoverPageURL(self, htid, pageNumber):
        self.uri = '{}/volume/pageimage/{}/{}?format=jpeg&v=2'.format(
            self.apiRoot, htid, pageNumber
        )
        self.mediaType = 'image/jpeg'

    def downloadCoverFile(self):
        hathiCoverResponse = self.makeHathiReq(self.uri)

        if hathiCoverResponse:
            return hathiCoverResponse.content

    def generateAuth(self):
        return OAuth1(self.apiKey, client_secret=self.apiSecret, signature_type='query')

    def makeHathiReq(self, url):
        try:
            hathiResp = requests.get(url, auth=self.generateAuth(), timeout=5)
            hathiResp.raise_for_status()

            return hathiResp
        except (ReadTimeout, HTTPError):
            pass


class HathiPage:
    PAGE_FEATURES = set(['FRONT_COVER', 'TITLE', 'IMAGE_ON_PAGE', 'TABLE_OF_CONTENTS'])

    def __init__(self, pageData):
        self.pageData = pageData

        self.pageNumber = self.getPageNumber()
        self.pageFlags = self.getPageFlags()
        self.pageScore = self.getPageScore()

    def getPageNumber(self):
        return self.pageData.get('ORDER', 0)

    def getPageFlags(self):
        return set(self.pageData.get('LABEL', '').split(', '))

    def getPageScore(self):
        return len(list(self.pageFlags & self.PAGE_FEATURES))


class HathiCoverError(Exception):
    def __init__(self, message):
        self.message = message