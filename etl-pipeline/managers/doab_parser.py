import inspect
import json
import requests
import re
from requests.exceptions import ConnectionError, InvalidURL, MissingSchema, ReadTimeout

from logger import create_log
import managers.parsers as parsers

logger = create_log(__name__)


class DOABLinkManager:
    def __init__(self, record):
        self.record = record
        self.manifests = []
        self.epub_links = []
        self.links_processed = []

        self.load_parsers()

    def load_parsers(self):
        parser_list = inspect.getmembers(parsers, inspect.isclass)

        self.parsers = [None] * len(parser_list)

        for parser in parser_list:
            _, parser_class = parser
            self.parsers[parser_class.ORDER - 1] = parser_class

    def select_parser(self, uri, media_type):
        root_uri, root_media_type = self.find_final_uri(uri, media_type)

        for parser_class in self.parsers:
            parser = parser_class(root_uri, root_media_type, self.record)

            if parser.validate_uri() is True:
                return parser

    def parse_links(self):
        parsed_links = []
        for part in self.record.has_part:
            part_no, part_uri, part_source, part_type, part_flags = list(
                part.split("|")
            )

            try:
                parser = self.select_parser(part_uri, part_type)
            except LinkError:
                logger.debug(f"Unable to parse link {part_uri}")
                continue

            if parser.uri in self.links_processed:
                continue

            self.links_processed.append(parser.uri)

            for parser_link in parser.create_links():
                parser_uri, parser_flags, parser_type, manifest, epub_file = parser_link

                if parser_flags:
                    part_flag_dict = json.loads(part_flags)
                    part_flag_dict.update(parser_flags)
                    part_flags = json.dumps(part_flag_dict)

                parsed_links.append(
                    "|".join(
                        [part_no, parser_uri, part_source, parser_type, part_flags]
                    )
                )

                if manifest:
                    self.manifests.append(manifest)

                if epub_file:
                    self.epub_links.append(epub_file)

        self.record.has_part = parsed_links

    @staticmethod
    def find_final_uri(uri, media_type, redirects=0):
        max_redirects = 5

        if redirects > max_redirects:
            return (uri, media_type)

        try:
            uri_header = requests.head(uri, allow_redirects=False, timeout=15)
            headers = dict(
                (key.lower(), value) for key, value in uri_header.headers.items()
            )
        except (
            MissingSchema,
            ConnectionError,
            InvalidURL,
            ReadTimeout,
            UnicodeDecodeError,
        ):
            raise LinkError("Invalid has_part URI")

        try:
            content_header = headers["content-type"]
            media_type = list(content_header.split(";"))[0].strip()
        except KeyError:
            content_header = None

        if uri_header.status_code in [301, 302, 307, 308] and content_header not in [
            "application/pdf",
            "application/epub+zip",
        ]:
            redirect_uri = headers["location"]

            if redirect_uri[0] == "/":
                uri_root = re.split(r"(?<![\/:])\/{1}", uri)[0]
                redirect_uri = "{}{}".format(uri_root, redirect_uri)

            redirects += 1

            return DOABLinkManager.find_final_uri(redirect_uri, media_type, redirects)

        return (uri, media_type)


class LinkError(Exception):
    def __init__(self, message):
        self.message = message
