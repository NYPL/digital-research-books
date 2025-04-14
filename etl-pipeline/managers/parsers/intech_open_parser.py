from managers.parsers.parser_abc import ParserABC
from managers.webpubManifest import WebpubManifest
import re

class InTechOpenParser(ParserABC):
    ORDER = 6

    def __init__(self, uri, media_type, record):
        super().__init__(uri, media_type, record)

        self.source = self.record.source
        self.identifier = list(self.record.identifiers[0].split('|'))[0]

    def validate_uri(self):
        intech_regex = r'intechopen.com'
        match = re.search(intech_regex, self.uri)

        if match:
            return True
        else:
            return False

    def create_links(self):
        s3_root = self.generate_s3_root()

        if self.media_type == 'application/pdf':
            manifest_path = 'manifests/{}/{}.json'.format(self.source, self.identifier)
            manifest_uri = '{}{}'.format(s3_root, manifest_path)

            manifest_json = self.generate_manifest(self.uri, manifest_uri)

            html_uri = self.create_html_uri()

            # QUESTIONS
            # 1. Do all Intechopen books have PDF links?
            # 2. How do we handle potential errors/add a fallback condition
            # 3. Are PDF links formatted consistently?

            if html_uri != None:

                return [
                    (manifest_uri, {'reader': True}, 'application/webpub+json', (manifest_path, manifest_json), None),
                    (self.uri, {'reader': False, 'download': True}, 'application/pdf',  None, None),
                    (html_uri, {'reader': False}, 'text/html', None, None)
                ]
            else:

                return [
                    (manifest_uri, {'reader': True}, 'application/webpub+json', (manifest_path, manifest_json), None),
                    (self.uri, {'reader': False, 'download': True}, 'application/pdf', None, None)
                ]

        elif self.media_type == 'text/html':
            return []

        return []


    def create_html_uri(self):
        '''
        Using regular expressions to search for identifier in PDF and parse it into HTML URI
        '''
        
        ident_regex = r'intechopen.com\/storage\/books\/([\d]+)'
        match = re.search(ident_regex, self.uri)

        if match != None:
            identifier = match.group(1) 

            html_uri = f'www.intechopen.com/books/{identifier}'

        else:
            html_uri = None

        return html_uri

    def generate_manifest(self, source_uri, manifest_uri):
        return super().generate_manifest(source_uri, manifest_uri)

    def generate_s3_root(self):
        return super().generate_s3_root()
