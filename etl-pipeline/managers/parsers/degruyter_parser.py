import re
import requests

from managers.parsers.parser_abc import ParserABC


class DeGruyterParser(ParserABC):
    ORDER = 5
    REGEX = r'www\.degruyter\.com\/.+\/[0-9]+'

    def __init__(self, uri, media_ype, record):
        super().__init__(uri, media_ype, record)

        self.uri_identifier = None

    def validate_uri(self):
        return True if re.search(self.REGEX, self.uri) else False

    def create_links(self):
        s3_root = self.generate_s3_root()

        title_match = re.search(r'\/title\/([0-9]+)(?:$|\?)', self.uri)
        isbn_match = re.search(r'\/(document\/doi\/10\.[0-9]+\/([0-9]+))\/html', self.uri)

        if title_match:
            self.uri_identifier = title_match.group(1)
            epub_source_uri = 'https://www.degruyter.com/downloadepub/title/{}'.format(self.uri_identifier)
            pdf_source_uri = 'https://www.degruyter.com/downloadpdf/title/{}'.format(self.uri_identifier)
        elif isbn_match:
            root_path = isbn_match.group(1)
            self.uri_identifier = isbn_match.group(2)
            epub_source_uri = 'https://www.degruyter.com/{}/epub'.format(root_path)
            pdf_source_uri = 'https://www.degruyter.com/{}/pdf'.format(root_path)

        if title_match or isbn_match:
            pdf_link = self.generate_pdf_links(s3_root, pdf_source_uri)
            epub_links = self.generate_epub_links(s3_root, epub_source_uri)

            return list(filter(None, [*pdf_link, *epub_links]))

        return super().create_links()
        
    def generate_epub_links(self, s3_root, epub_source_uri):
        epub_head_status, _ = DeGruyterParser.make_head_query(epub_source_uri)

        if epub_head_status != 200:
            return [None]

        epub_download_path = 'epubs/degruyter/{}.epub'.format(self.uri_identifier)
        epub_download_uri = '{}{}'.format(s3_root, epub_download_path)

        epub_read_path = 'epubs/degruyter/{}/META-INF/container.xml'.format(self.uri_identifier)
        epub_read_uri = '{}{}'.format(s3_root, epub_read_path)

        webpub_read_path = 'epubs/degruyter/{}/manifest.json'.format(self.uri_identifier)
        webpub_read_uri = '{}{}'.format(s3_root, webpub_read_path)

        return [
            (webpub_read_uri, {'reader': True}, 'application/webpub+json', None, None),
            (epub_read_uri, {'reader': True}, 'application/epub+xml', None, None),
            (epub_download_uri, {'download': True}, 'application/epub+zip', None, (epub_download_path, epub_source_uri)),
        ]

    def generate_pdf_links(self, s3_root, pdf_source_uri):
        manifest_path = 'manifests/degruyter/{}.json'.format(self.uri_identifier)
        manifest_uri = '{}{}'.format(s3_root, manifest_path)
        manifest_json = self.generate_manifest(pdf_source_uri, manifest_uri)

        return [
            (manifest_uri, {'reader': True}, 'application/webpub+json', (manifest_path, manifest_json), None),
            (pdf_source_uri, {'download': True}, 'application/pdf', None, None)
        ]

    def generate_manifest(self, source_uri, manifest_uri):
        return super().generate_manifest(source_uri, manifest_uri)

    def generate_s3_root(self):
        return super().generate_s3_root()

    @staticmethod
    def make_head_query(uri):
        head_resp = requests.head(
            uri, timeout=DeGruyterParser.TIMEOUT,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
        )

        return (head_resp.status_code, head_resp.headers)
