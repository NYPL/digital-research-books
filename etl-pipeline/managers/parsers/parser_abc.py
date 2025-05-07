from abc import ABC, abstractmethod
import os
import re

from managers.webpubManifest import WebpubManifest


class ParserABC(ABC):
    TIMEOUT = 15

    @abstractmethod
    def __init__(self, uri, media_type, record):
        self.uri = uri
        self.media_type = media_type
        self.record = record

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        if not re.match(r"https?:\/\/", value):
            self._uri = "http://{}".format(value)
        else:
            self._uri = value

    @abstractmethod
    def validate_uri(self):
        return True

    @abstractmethod
    def create_links(self):
        return [(self.uri, None, self.media_type, None, None)]

    @abstractmethod
    def generate_manifest(self, source_uri, manifest_uri):
        manifest = WebpubManifest(source_uri, "application/pdf")

        manifest.addMetadata(self.record)

        manifest.addChapter(source_uri, self.record.title)

        manifest.links.append(
            {"rel": "self", "href": manifest_uri, "type": "application/webpub+json"}
        )

        return manifest.toJson()

    @abstractmethod
    def generate_s3_root(self):
        bucket = os.environ["FILE_BUCKET"]
        endpoint = os.environ.get("S3_ENDPOINT_URL", None)

        if endpoint:
            return f"{endpoint}/{bucket}/"

        return "https://{}.s3.amazonaws.com/".format(bucket)
