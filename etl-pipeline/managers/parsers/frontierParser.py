import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class FrontierParser(ParserABC):
    ORDER = 3
    REGEX = r'(?:www|journal)\.frontiersin\.org\/research-topics\/([0-9]+)\/([a-zA-Z0-9\-]+)'

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.uriIdentifier = None

    def validateURI(self):
        try:
            match = re.search(self.REGEX, self.uri)
            self.uriIdentifier = match.group(1)

            return True
        except (IndexError, AttributeError):
            return False

    def createLinks(self):
        s3Root = self.generateS3Root()

        pdfLink = self.generatePDFLinks(s3Root)

        ePubLinks = self.generateEpubLinks(s3Root)

        return list(filter(None, [*pdfLink, *ePubLinks]))
        
    def generateEpubLinks(self, s3Root):
        ePubSourceURI = 'https://www.frontiersin.org/research-topics/{}/epub'.format(self.uriIdentifier)

        frontierStatus, frontierHeaders = FrontierParser.checkAvailability(ePubSourceURI)

        if frontierStatus != 200: return [None]

        try:
            contentHeader = frontierHeaders.get('content-disposition', '')
            filename = re.search(r'filename=(.+)\.EPUB$', contentHeader).group(1)
        except (AttributeError, KeyError):
            return [None]

        ePubDownloadPath = 'epubs/frontier/{}_{}.epub'.format(self.uriIdentifier, filename)
        ePubDownloadURI = '{}{}'.format(s3Root, ePubDownloadPath)

        ePubReadPath = 'epubs/frontier/{}_{}/META-INF/container.xml'.format(self.uriIdentifier, filename)
        ePubReadURI = '{}{}'.format(s3Root, ePubReadPath)

        webpubReadPath = 'epubs/frontier/{}_{}/manifest.json'.format(self.uriIdentifier, filename)
        webpubReadURI = '{}{}'.format(s3Root, webpubReadPath)

        return [
            (webpubReadURI, {'reader': True}, 'application/webpub+json', None, None),
            (ePubReadURI, {'reader': True}, 'application/epub+xml', None, None),
            (ePubDownloadURI, {'download': True}, 'application/epub+zip', None, (ePubDownloadPath, ePubSourceURI))
        ]

    def generatePDFLinks(self, s3Root):
        pdfSourceURI = 'https://www.frontiersin.org/research-topics/{}/pdf'.format(self.uriIdentifier)
        manifestPath = 'manifests/frontier/{}.json'.format(self.uriIdentifier)
        manifestURI = '{}{}'.format(s3Root, manifestPath)
        manifestJSON = self.generateManifest(pdfSourceURI, manifestURI)

        return [
            (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
            (pdfSourceURI, {'download': True}, 'application/pdf', None, None),
        ]

    def generateManifest(self, sourceURI, manifestURI):
        return super().generateManifest(sourceURI, manifestURI)

    def generateS3Root(self):
        return super().generateS3Root()

    @staticmethod
    def checkAvailability(uri):
        try:
            headResp = requests.head(
                uri,
                timeout=FrontierParser.TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
            )

            return (headResp.status_code, headResp.headers)
        except ReadTimeout:
            return (0, None)
