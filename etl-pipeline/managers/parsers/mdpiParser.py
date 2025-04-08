import re

from managers.parsers.parser_abc import ParserABC


class MDPIParser(ParserABC):
    ORDER = 4
    REGEX = r'mdpi.com/books/pdfview/book/([0-9]+)$'

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.identifier = list(self.record.identifiers[0].split('|'))[0]

    def validateURI(self):
        return True if re.search(self.REGEX, self.uri) else False

    def createLinks(self):
        s3Root = self.generateS3Root()

        return self.generatePDFLinks(s3Root)
        
    def generatePDFLinks(self, s3Root):
        pdfSourceURI = self.uri.replace('pdfview', 'pdfdownload')
        manifestPath = 'manifests/mdpi/{}.json'.format(self.identifier)
        manifestURI = '{}{}'.format(s3Root, manifestPath)
        manifestJSON = self.generateManifest(pdfSourceURI, manifestURI)

        return [
            (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
            (pdfSourceURI, {'download': True}, 'application/pdf', None, None)
        ]

    def generateManifest(self, sourceURI, manifestURI):
        return super().generateManifest(sourceURI, manifestURI)

    def generateS3Root(self):
        return super().generateS3Root()
