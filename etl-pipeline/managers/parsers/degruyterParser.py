import re
import requests

from managers.parsers.parser_abc import ParserABC


class DeGruyterParser(ParserABC):
    ORDER = 5
    REGEX = r'www\.degruyter\.com\/.+\/[0-9]+'

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.uriIdentifier = None

    def validateURI(self):
        return True if re.search(self.REGEX, self.uri) else False

    def createLinks(self):
        s3Root = self.generateS3Root()

        titleMatch = re.search(r'\/title\/([0-9]+)(?:$|\?)', self.uri)
        isbnMatch = re.search(r'\/(document\/doi\/10\.[0-9]+\/([0-9]+))\/html', self.uri)

        if titleMatch:
            self.uriIdentifier = titleMatch.group(1)
            ePubSourceURI = 'https://www.degruyter.com/downloadepub/title/{}'.format(self.uriIdentifier)
            pdfSourceURI = 'https://www.degruyter.com/downloadpdf/title/{}'.format(self.uriIdentifier)
        elif isbnMatch:
            rootPath = isbnMatch.group(1)
            self.uriIdentifier = isbnMatch.group(2)
            ePubSourceURI = 'https://www.degruyter.com/{}/epub'.format(rootPath)
            pdfSourceURI = 'https://www.degruyter.com/{}/pdf'.format(rootPath)

        if titleMatch or isbnMatch:
            pdfLink = self.generatePDFLinks(s3Root, pdfSourceURI)
            ePubLinks = self.generateEpubLinks(s3Root, ePubSourceURI)

            return list(filter(None, [*pdfLink, *ePubLinks]))

        return super().createLinks()
        
    def generateEpubLinks(self, s3Root, ePubSourceURI):
        ePubHeadStatus, _ = DeGruyterParser.makeHeadQuery(ePubSourceURI)

        if ePubHeadStatus != 200:
            return [None]

        ePubDownloadPath = 'epubs/degruyter/{}.epub'.format(self.uriIdentifier)
        ePubDownloadURI = '{}{}'.format(s3Root, ePubDownloadPath)

        ePubReadPath = 'epubs/degruyter/{}/META-INF/container.xml'.format(self.uriIdentifier)
        ePubReadURI = '{}{}'.format(s3Root, ePubReadPath)

        webpubReadPath = 'epubs/degruyter/{}/manifest.json'.format(self.uriIdentifier)
        webpubReadURI = '{}{}'.format(s3Root, webpubReadPath)

        return [
            (webpubReadURI, {'reader': True}, 'application/webpub+json', None, None),
            (ePubReadURI, {'reader': True}, 'application/epub+xml', None, None),
            (ePubDownloadURI, {'download': True}, 'application/epub+zip', None, (ePubDownloadPath, ePubSourceURI)),
        ]

    def generatePDFLinks(self, s3Root, pdfSourceURI):
        manifestPath = 'manifests/degruyter/{}.json'.format(self.uriIdentifier)
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

    @staticmethod
    def makeHeadQuery(uri):
        headResp = requests.head(
            uri, timeout=DeGruyterParser.TIMEOUT,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
        )

        return (headResp.status_code, headResp.headers)
