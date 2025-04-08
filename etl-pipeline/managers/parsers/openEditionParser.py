from bs4 import BeautifulSoup
import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class OpenEditionParser(ParserABC):
    ORDER = 2
    REGEX = r'books.openedition.org/([a-z0-9]+)/([0-9]+)$'
    FORMATS = [
        {'regex': r'\/(epub\/[0-9]+)', 'mediaType': 'application/epub+xml', 'flags': {'reader': True}},
        {'regex': r'\/(pdf\/[0-9]+)', 'mediaType': 'application/webpub+json', 'flags': {'reader': True}},
        {'regex': r'([0-9]+\?format=reader)$', 'mediaType': 'text/html', 'flags': {}},
        {'regex': r'([0-9]+)$', 'mediaType': 'text/html', 'flags': {}},
    ]

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.publisher = None
        self.uriIdentifier = None

    def validateURI(self):
        try:
            match = re.search(self.REGEX, self.uri)
            self.publisher = match.group(1)
            self.uriIdentifier = match.group(2)

            if match.start() > 8:
                self.uri = self.uri[match.start():]

            return True
        except (IndexError, AttributeError):
            return False

    def createLinks(self):
        outLinks = []

        for bookLink in self.loadEbookLinks():
            bookURI, bookType, bookFlags = bookLink

            if bookType == 'application/epub+xml':
               outLinks.extend(self.createEpubLink(bookURI, bookType, bookFlags)) 
            elif bookType == 'application/webpub+json':
                outLinks.extend(self.createPDFLink(bookURI, bookType, bookFlags))
            else:
                outLinks.append((bookURI, bookFlags, bookType, None, None))

        return outLinks

    def createPDFLink(self, bookURI, bookType, bookFlags):
        s3Root = self.generateS3Root()

        manifestPath = 'manifests/doab/{}_{}.json'.format(self.publisher, self.uriIdentifier)
        manifestURI = '{}{}'.format(s3Root, manifestPath)
        manifestJSON = self.generateManifest(bookURI, manifestURI)

        return [
            (manifestURI, bookFlags, bookType, (manifestPath, manifestJSON), None),
            (bookURI, {'download': True}, 'application/pdf', None, None)
        ]

    def createEpubLink(self, bookURI, bookType, bookFlags):
        s3Root = self.generateS3Root()

        ePubDownloadPath = 'epubs/doab/{}_{}.epub'.format(self.publisher, self.uriIdentifier)
        ePubDownloadURI = '{}{}'.format(s3Root, ePubDownloadPath)

        ePubReadPath = 'epubs/doab/{}_{}/META-INF/container.xml'.format(self.publisher, self.uriIdentifier)
        ePubReadURI = '{}{}'.format(s3Root, ePubReadPath)

        webpubReadPath = 'epubs/doab/{}_{}/manifest.json'.format(self.publisher, self.uriIdentifier)
        webpubReadURI = '{}{}'.format(s3Root, webpubReadPath)

        return [
            (webpubReadURI, bookFlags, 'application/webpub+json', None, None),
            (ePubReadURI, bookFlags, bookType, None, None),
            (ePubDownloadURI, {'download': True}, 'application/epub+zip', None, (ePubDownloadPath, bookURI))
        ]

    def loadEbookLinks(self):
        try:
            oeResp = requests.get(self.uri, timeout=self.TIMEOUT)
        except ReadTimeout:
            return []
        
        if oeResp.status_code != 200: return []

        oePage = BeautifulSoup(oeResp.text, 'html.parser')

        accessElement = oePage.find(id='book-access')

        if not accessElement:
            return []

        accessLinks = accessElement.find_all('a')

        return list(filter(None, [self.parseBookLink(l) for l in accessLinks]))

    def parseBookLink(self, link):
        linkRel = link.get('href')

        for format in self.FORMATS:
            formatMatch = re.search(format['regex'], linkRel)
            
            if formatMatch:
                return (
                    '{}/{}/{}'.format('http://books.openedition.org', self.publisher, formatMatch.group(1)),
                    format['mediaType'],
                    format['flags']
                )

    def generateManifest(self, sourceURI, manifestURI):
        return super().generateManifest(sourceURI, manifestURI)

    def generateS3Root(self):
        return super().generateS3Root()
        