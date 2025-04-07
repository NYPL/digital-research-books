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

        self.api_root = 'https://babel.hathitrust.org/cgi/htd'
        self.api_key = os.environ['HATHI_API_KEY']
        self.api_secret = os.environ['HATHI_API_SECRET']

        self.uri = None
        self.media_type = None

    def has_cover(self):
        for value, source in self.identifiers:
            if source != 'hathi' or '.' not in value: continue

            try:
                self.fetch_volume_cover(value)

                self.cover_id = value

                return True
            except HathiCoverError as e:
                logger.error('Unable to parse HathiTrust volume for cover')
                logger.debug(e.message)

        return False

    def fetch_volume_cover(self, htid):
        logger.info(f'Fetching hathi cover for {htid}')

        mets_url = '{}/structure/{}?format=json&v=2'.format(self.api_root, htid)

        response = self.make_hathi_req(mets_url)

        if not response:
            raise HathiCoverError('Invalid htid {}'.format(htid))

        response_json = response.json()

        try:
            page_list = response_json['METS:structMap']['METS:div']['METS:div'][:25]
        except (TypeError, KeyError):
            logger.debug(response_json)
            raise HathiCoverError('Unexpected METS format in hathi rec {}'.format(htid))

        ranked_mets_pages = sorted(
            [HathiPage(page) for page in page_list], key=lambda x: x.page_score, reverse=True
        )

        self.set_cover_page_url(htid, ranked_mets_pages[0].page_number)

    def set_cover_page_url(self, htid, pageNumber):
        self.uri = '{}/volume/pageimage/{}/{}?format=jpeg&v=2'.format(
            self.api_root, htid, pageNumber
        )
        self.media_type = 'image/jpeg'

    def download_cover_file(self):
        response = self.make_hathi_req(self.uri)

        if response:
            return response.content

    def generate_auth(self):
        return OAuth1(self.api_key, client_secret=self.api_secret, signature_type='query')

    def make_hathi_req(self, url):
        try:
            response = requests.get(url, auth=self.generate_auth(), timeout=5)
            response.raise_for_status()

            return response
        except (ReadTimeout, HTTPError):
            pass


class HathiPage:
    PAGE_FEATURES = set(['FRONT_COVER', 'TITLE', 'IMAGE_ON_PAGE', 'TABLE_OF_CONTENTS'])

    def __init__(self, page_data):
        self.page_data = page_data

        self.page_number = self.get_page_number()
        self.page_flags = self.get_page_flags()
        self.page_score = self.get_page_score()

    def get_page_number(self):
        return self.page_data.get('ORDER', 0)

    def get_page_flags(self):
        return set(self.page_data.get('LABEL', '').split(', '))

    def get_page_score(self):
        return len(list(self.page_flags & self.PAGE_FEATURES))


class HathiCoverError(Exception):
    def __init__(self, message):
        self.message = message