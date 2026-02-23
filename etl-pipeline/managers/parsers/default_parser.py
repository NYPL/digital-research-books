from managers.parsers.parser_abc import ParserABC
from managers.webpubManifest import WebpubManifest


class DefaultParser(ParserABC):
    ORDER = 7

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.source = self.record.source
        self.identifier = list(self.record.identifiers[0].split("|"))[0]

    def validate_uri(self):
        return super().validate_uri()

    def create_links(self):
        s3_root = self.generate_s3_root()

        if self.media_type == "application/pdf":
            manifest_path = "manifests/{}/{}.json".format(self.source, self.identifier)
            manifest_uri = "{}{}".format(s3_root, manifest_path)

            manifest_json = self.generate_manifest(self.uri, manifest_uri)

            return [
                (
                    manifest_uri,
                    {"reader": True},
                    "application/webpub+json",
                    (manifest_path, manifest_json),
                    None,
                ),
                (
                    self.uri,
                    {"reader": False, "download": True},
                    self.media_type,
                    None,
                    None,
                ),
            ]
        elif self.media_type == "application/epub+zip":
            epub_download_path = "epubs/{}/{}.epub".format(self.source, self.identifier)
            epub_download_uri = "{}{}".format(s3_root, epub_download_path)

            epub_read_path = "epubs/{}/{}/META-INF/container.xml".format(
                self.source, self.identifier
            )
            epub_read_uri = "{}{}".format(s3_root, epub_read_path)

            webpub_read_path = "epubs/{}/{}/manifest.json".format(
                self.source, self.identifier
            )
            webpub_read_uri = "{}{}".format(s3_root, webpub_read_path)

            return [
                (
                    webpub_read_uri,
                    {"reader": True},
                    "application/webpub+json",
                    None,
                    None,
                ),
                (epub_read_uri, {"reader": True}, "application/epub+xml", None, None),
                (
                    epub_download_uri,
                    {"download": True},
                    self.media_type,
                    None,
                    (epub_download_path, self.uri),
                ),
            ]

        return super().create_links()

    def generate_manifest(self, source_uri, manifest_uri):
        return super().generate_manifest(source_uri, manifest_uri)

    def generate_s3_root(self):
        return super().generate_s3_root()
