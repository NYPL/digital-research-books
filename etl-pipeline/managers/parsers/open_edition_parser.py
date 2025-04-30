from bs4 import BeautifulSoup
import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class OpenEditionParser(ParserABC):
    ORDER = 2
    REGEX = r"books.openedition.org/([a-z0-9]+)/([0-9]+)$"
    FORMATS = [
        {
            "regex": r"\/(epub\/[0-9]+)",
            "mediaType": "application/epub+xml",
            "flags": {"reader": True},
        },
        {
            "regex": r"\/(pdf\/[0-9]+)",
            "mediaType": "application/webpub+json",
            "flags": {"reader": True},
        },
        {"regex": r"([0-9]+\?format=reader)$", "mediaType": "text/html", "flags": {}},
        {"regex": r"([0-9]+)$", "mediaType": "text/html", "flags": {}},
    ]

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.publisher = None
        self.uri_identifier = None

    def validate_uri(self):
        try:
            match = re.search(self.REGEX, self.uri)
            self.publisher = match.group(1)
            self.uri_identifier = match.group(2)

            if match.start() > 8:
                self.uri = self.uri[match.start() :]

            return True
        except (IndexError, AttributeError):
            return False

    def create_links(self):
        out_links = []

        for book_link in self.load_ebook_links():
            book_uri, book_type, book_flags = book_link

            if book_type == "application/epub+xml":
                out_links.extend(self.create_epub_link(book_uri, book_type, book_flags))
            elif book_type == "application/webpub+json":
                out_links.extend(self.create_pdf_link(book_uri, book_type, book_flags))
            else:
                out_links.append((book_uri, book_flags, book_type, None, None))

        return out_links

    def create_pdf_link(self, book_uri, book_type, book_flags):
        s3_root = self.generate_s3_root()

        manifest_path = "manifests/doab/{}_{}.json".format(
            self.publisher, self.uri_identifier
        )
        manifest_uri = "{}{}".format(s3_root, manifest_path)
        manifest_json = self.generate_manifest(book_uri, manifest_uri)

        return [
            (manifest_uri, book_flags, book_type, (manifest_path, manifest_json), None),
            (book_uri, {"download": True}, "application/pdf", None, None),
        ]

    def create_epub_link(self, book_uri, book_type, book_flags):
        s3_root = self.generate_s3_root()

        ePub_download_path = "epubs/doab/{}_{}.epub".format(
            self.publisher, self.uri_identifier
        )
        ePub_download_uri = "{}{}".format(s3_root, ePub_download_path)

        ePub_read_path = "epubs/doab/{}_{}/META-INF/container.xml".format(
            self.publisher, self.uri_identifier
        )
        ePub_read_uri = "{}{}".format(s3_root, ePub_read_path)

        webpub_read_path = "epubs/doab/{}_{}/manifest.json".format(
            self.publisher, self.uri_identifier
        )
        webpub_read_uri = "{}{}".format(s3_root, webpub_read_path)

        return [
            (webpub_read_uri, book_flags, "application/webpub+json", None, None),
            (ePub_read_uri, book_flags, book_type, None, None),
            (
                ePub_download_uri,
                {"download": True},
                "application/epub+zip",
                None,
                (ePub_download_path, book_uri),
            ),
        ]

    def load_ebook_links(self):
        try:
            response = requests.get(self.uri, timeout=self.TIMEOUT)
        except ReadTimeout:
            return []

        if response.status_code != 200:
            return []

        oe_page = BeautifulSoup(response.text, "html.parser")

        access_element = oe_page.find(id="book-access")

        if not access_element:
            return []

        access_links = access_element.find_all("a")

        return list(filter(None, [self.parse_book_link(l) for l in access_links]))

    def parse_book_link(self, link):
        link_rel = link.get("href")

        for format in self.FORMATS:
            format_match = re.search(format["regex"], link_rel)

            if format_match:
                return (
                    "{}/{}/{}".format(
                        "http://books.openedition.org",
                        self.publisher,
                        format_match.group(1),
                    ),
                    format["mediaType"],
                    format["flags"],
                )

    def generate_manifest(self, source_uri, manifest_uri):
        return super().generate_manifest(source_uri, manifest_uri)

    def generate_s3_root(self):
        return super().generate_s3_root()
