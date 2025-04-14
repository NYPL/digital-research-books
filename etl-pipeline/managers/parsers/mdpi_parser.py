import re

from managers.parsers.parser_abc import ParserABC


class MDPIParser(ParserABC):
    ORDER = 4
    REGEX = r'mdpi.com/books/pdfview/book/([0-9]+)$'

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.identifier = list(self.record.identifiers[0].split('|'))[0]

    def validate_uri(self):
        return True if re.search(self.REGEX, self.uri) else False

    def create_links(self):
        s3_root = self.generate_s3_root()

        return self.generate_pdf_links(s3_root)
        
    def generate_pdf_links(self, s3_root):
        pdf_source_uri = self.uri.replace('pdfview', 'pdfdownload')
        manifest_path = 'manifests/mdpi/{}.json'.format(self.identifier)
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
