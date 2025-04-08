from bs4 import BeautifulSoup
import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class SpringerParser(ParserABC):
    ORDER = 1
    REGEX = r'link.springer.com\/book\/(10\.[0-9]+)(?:\/|\%2F)([0-9\-]+)'
    REDIRECT_REGEX = r'((?:https?:\/\/)?link\.springer\.com\/.+)$'
    STORE_REGEX = r'springer.com\/gp\/book\/(978[0-9]+)$'

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.code = None
        self.uri_identifier = None

    def validate_uri(self):
        try:
            match = re.search(self.REGEX, self.uri)

            self.code = match.group(1)
            self.uri_identifier = match.group(2)

            return True
        except (IndexError, AttributeError):
            if 'springer' in self.uri:
                return self.validate_alt_link_formats()
            
            return False

    def validate_alt_link_formats(self):
        if re.search(self.STORE_REGEX, self.uri):
            self.find_oa_link()

        redirect_match = re.search(self.REDIRECT_REGEX, self.uri)

        if redirect_match:
            self.uri = redirect_match.group(1)

            try:
                redirect_header = requests.head(self.uri, timeout=self.TIMEOUT)

                self.uri = redirect_header.headers['Location']
                return self.validate_uri()
            except (KeyError, ReadTimeout):
                pass
        
        return False

    def find_oa_link(self):
        try:
            response = requests.get(self.uri, timeout=self.TIMEOUT)

            if response.status_code == 200:
                store_page = BeautifulSoup(response.text, 'html.parser')

                access_link = store_page.find(class_='openaccess')

                if access_link: self.uri = access_link.get('href')
        except ReadTimeout:
            pass

    def create_links(self):
        s3_root = self.generate_s3_root()

        pdf_links = self.create_pdf_links(s3_root)

        epub_links = self.create_epub_links(s3_root)

        return [*pdf_links, *epub_links]

    def create_pdf_links(self, s3_root):
        pdf_source_uri = 'https://link.springer.com/content/pdf/{}/{}.pdf'.format(self.code, self.uri_identifier)

        if SpringerParser.check_availability(pdf_source_uri) is False: return []

        manifest_path = 'manifests/springer/{}_{}.json'.format(
            self.code.replace('.', '-'), self.uri_identifier
        )
        manifest_uri = '{}{}'.format(s3_root, manifest_path)
        manifest_json = self.generate_manifest(pdf_source_uri, manifest_uri)

        return [
            (manifest_uri, {'reader': True}, 'application/webpub+json', (manifest_path, manifest_json), None),
            (pdf_source_uri, {'download': True}, 'application/pdf', None, None)
        ]

    def create_epub_links(self, s3_root):
        epub_source_uri = 'https://link.springer.com/download/epub/{}/{}.epub'.format(self.code, self.uri_identifier)

        if SpringerParser.check_availability(epub_source_uri) is False: return []

        uri_code = self.code.replace('.', '-')

        epub_download_path = 'epubs/springer/{}_{}.epub'.format(
            uri_code, self.uri_identifier
        )
        epub_download_uri = '{}{}'.format(s3_root, epub_download_path)
        epub_read_path = 'epubs/springer/{}_{}/META-INF/container.xml'.format(
            uri_code, self.uri_identifier
        )
        epub_read_uri = '{}{}'.format(s3_root, epub_read_path)

        webpub_read_path = 'epubs/springer/{}_{}/manifest.json'.format(
            uri_code, self.uri_identifier
        )
        webpub_read_uri = '{}{}'.format(s3_root, webpub_read_path)

        return [
            (webpub_read_uri, {'reader': True}, 'application/webpub+json', None, None),
            (epub_read_uri, {'reader': True}, 'application/epub+zip', None, None),
            (epub_download_uri, {'download': True}, 'application/epub+xml', None, (epub_download_path, epub_source_uri))
        ]

    def generate_manifest(self, source_uri, manifest_uri):
        return super().generate_manifest(source_uri, manifest_uri)

    def generate_s3_root(self):
        return super().generate_s3_root()

    @staticmethod
    def check_availability(uri):
        try:
            response = requests.head(
                uri,
                timeout=SpringerParser.TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
            )
        except ReadTimeout:
            return False

        return response.status_code == 200
        