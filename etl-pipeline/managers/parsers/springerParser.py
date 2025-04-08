from bs4 import BeautifulSoup
import re
import requests
from requests.exceptions import ReadTimeout

from managers.parsers.parser_abc import ParserABC


class SpringerParser(ParserABC):
    ORDER = 1
    REGEX = r'link.springer.com\/book\/(10\.[0-9]+)(?:\/|\%2F)([0-9\-]+)'
    REDIRECT_REGEX = r'((?:https?:\/\/)?link\.springer\.com\/.+)$'
    STORE_REGEX = r'springer.com\/gp\/book\/(978[0-9]+)$'

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.code = None
        self.uriIdentifier = None

    def validateURI(self):
        try:
            match = re.search(self.REGEX, self.uri)

            self.code = match.group(1)
            self.uriIdentifier = match.group(2)

            return True
        except (IndexError, AttributeError):
            if 'springer' in self.uri:
                return self.validateAltLinkFormats()
            
            return False

    def validateAltLinkFormats(self):
        if re.search(self.STORE_REGEX, self.uri):
            self.findOALink()

        redirectMatch = re.search(self.REDIRECT_REGEX, self.uri)

        if redirectMatch:
            self.uri = redirectMatch.group(1)

            try:
                redirectHeader = requests.head(self.uri, timeout=self.TIMEOUT)

                self.uri = redirectHeader.headers['Location']
                return self.validateURI()
            except (KeyError, ReadTimeout):
                pass
        
        return False

    def findOALink(self):
        try:
            storeResp = requests.get(self.uri, timeout=self.TIMEOUT)

            if storeResp.status_code == 200:
                storePage = BeautifulSoup(storeResp.text, 'html.parser')

                accessLink = storePage.find(class_='openaccess')

                if accessLink: self.uri = accessLink.get('href')
        except ReadTimeout:
            pass

    def createLinks(self):
        s3Root = self.generateS3Root()

        pdfLinks = self.createPDFLinks(s3Root)

        ePubLinks = self.createEPubLinks(s3Root)

        return [*pdfLinks, *ePubLinks]

    def createPDFLinks(self, s3Root):
        pdfSourceURI = 'https://link.springer.com/content/pdf/{}/{}.pdf'.format(self.code, self.uriIdentifier)

        if SpringerParser.checkAvailability(pdfSourceURI) is False: return []

        manifestPath = 'manifests/springer/{}_{}.json'.format(
            self.code.replace('.', '-'), self.uriIdentifier
        )
        manifestURI = '{}{}'.format(s3Root, manifestPath)
        manifestJSON = self.generateManifest(pdfSourceURI, manifestURI)

        return [
            (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
            (pdfSourceURI, {'download': True}, 'application/pdf', None, None)
        ]

    def createEPubLinks(self, s3Root):
        ePubSourceURI = 'https://link.springer.com/download/epub/{}/{}.epub'.format(self.code, self.uriIdentifier)

        if SpringerParser.checkAvailability(ePubSourceURI) is False: return []

        uriCode = self.code.replace('.', '-')

        ePubDownloadPath = 'epubs/springer/{}_{}.epub'.format(
            uriCode, self.uriIdentifier
        )
        ePubDownloadURI = '{}{}'.format(s3Root, ePubDownloadPath)
        ePubReadPath = 'epubs/springer/{}_{}/META-INF/container.xml'.format(
            uriCode, self.uriIdentifier
        )
        ePubReadURI = '{}{}'.format(s3Root, ePubReadPath)

        webpubReadPath = 'epubs/springer/{}_{}/manifest.json'.format(
            uriCode, self.uriIdentifier
        )
        webpubReadURI = '{}{}'.format(s3Root, webpubReadPath)

        return [
            (webpubReadURI, {'reader': True}, 'application/webpub+json', None, None),
            (ePubReadURI, {'reader': True}, 'application/epub+zip', None, None),
            (ePubDownloadURI, {'download': True}, 'application/epub+xml', None, (ePubDownloadPath, ePubSourceURI))
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
                timeout=SpringerParser.TIMEOUT,
                headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
            )
        except ReadTimeout:
            return False

        return headResp.status_code == 200
        