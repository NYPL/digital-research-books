from managers.parsers.parser_abc import ParserABC
from managers.webpubManifest import WebpubManifest


class DefaultParser(ParserABC):
    ORDER = 7

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.source = self.record.source
        self.identifier = list(self.record.identifiers[0].split('|'))[0]

    def validateURI(self):
        return super().validateURI()

    def createLinks(self):
        s3Root = self.generateS3Root()

        if self.mediaType == 'application/pdf':
            manifestPath = 'manifests/{}/{}.json'.format(self.source, self.identifier)
            manifestURI = '{}{}'.format(s3Root, manifestPath)

            manifestJSON = self.generateManifest(self.uri, manifestURI)

            return [
                (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
                (self.uri, {'reader': False, 'download': True}, self.mediaType, None, None)
            ]
        elif self.mediaType == 'application/epub+zip':
            ePubDownloadPath = 'epubs/{}/{}.epub'.format(self.source, self.identifier)
            ePubDownloadURI = '{}{}'.format(s3Root, ePubDownloadPath)

            ePubReadPath = 'epubs/{}/{}/META-INF/container.xml'.format(self.source, self.identifier)
            ePubReadURI = '{}{}'.format(s3Root, ePubReadPath)

            webpubReadPath = 'epubs/{}/{}/manifest.json'.format(self.source, self.identifier)
            webpubReadURI = '{}{}'.format(s3Root, webpubReadPath)

            return [
                (webpubReadURI, {'reader': True}, 'application/webpub+json', None, None),
                (ePubReadURI, {'reader': True}, 'application/epub+xml', None, None),
                (ePubDownloadURI, {'download': True}, self.mediaType, None, (ePubDownloadPath, self.uri))
            ]

        return super().createLinks()

    def generateManifest(self, sourceURI, manifestURI):
        return super().generateManifest(sourceURI, manifestURI)

    def generateS3Root(self):
        return super().generateS3Root()
