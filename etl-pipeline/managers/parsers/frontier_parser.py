import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class FrontierParser(ParserABC):
    ORDER = 3
    REGEX = r"(?:www|journal)\.frontiersin\.org\/research-topics\/([0-9]+)\/([a-zA-Z0-9\-]+)"

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.uri_identifier = None

    def validate_uri(self):
        try:
            match = re.search(self.REGEX, self.uri)
            self.uri_identifier = match.group(1)

            return True
        except (IndexError, AttributeError):
            return False

    def create_links(self):
        s3_root = self.generate_s3_root()

        pdf_link = self.generate_pdf_links(s3_root)

        ePub_links = self.generate_epub_links(s3_root)

        return list(filter(None, [*pdf_link, *ePub_links]))

    def generate_epub_links(self, s3_root):
        epub_source_uri = "https://www.frontiersin.org/research-topics/{}/epub".format(
            self.uri_identifier
        )

        frontier_status, frontier_headers = FrontierParser.check_availability(
            epub_source_uri
        )

        if frontier_status != 200:
            return [None]

        try:
            content_header = frontier_headers.get("content-disposition", "")
            filename = re.search(r"filename=(.+)\.EPUB$", content_header).group(1)
        except (AttributeError, KeyError):
            return [None]

        epub_download_path = "epubs/frontier/{}_{}.epub".format(
            self.uri_identifier, filename
        )
        epub_download_uri = "{}{}".format(s3_root, epub_download_path)

        epub_read_path = "epubs/frontier/{}_{}/META-INF/container.xml".format(
            self.uri_identifier, filename
        )
        epub_read_uri = "{}{}".format(s3_root, epub_read_path)

        webpub_read_path = "epubs/frontier/{}_{}/manifest.json".format(
            self.uri_identifier, filename
        )
        webpub_read_uri = "{}{}".format(s3_root, webpub_read_path)

        return [
            (webpub_read_uri, {"reader": True}, "application/webpub+json", None, None),
            (epub_read_uri, {"reader": True}, "application/epub+xml", None, None),
            (
                epub_download_uri,
                {"download": True},
                "application/epub+zip",
                None,
                (epub_download_path, epub_source_uri),
            ),
        ]

    def generate_pdf_links(self, s3_root):
        pdf_source_uri = "https://www.frontiersin.org/research-topics/{}/pdf".format(
            self.uri_identifier
        )
        manifest_path = "manifests/frontier/{}.json".format(self.uri_identifier)
        manifest_uri = "{}{}".format(s3_root, manifest_path)
        manifest_json = self.generate_manifest(pdf_source_uri, manifest_uri)

        return [
            (
                manifest_uri,
                {"reader": True},
                "application/webpub+json",
                (manifest_path, manifest_json),
                None,
            ),
            (pdf_source_uri, {"download": True}, "application/pdf", None, None),
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
                timeout=FrontierParser.TIMEOUT,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)"
                },
            )

            return (response.status_code, response.headers)
        except ReadTimeout:
            return (0, None)
