from managers.parsers.parser_abc import ParserABC
from managers.webpubManifest import WebpubManifest
import re

class InTechOpenParser(ParserABC):
    ORDER = 6

    def __init__(self, uri, mediaType, record):
        super().__init__(uri, mediaType, record)

        self.source = self.record.source
        self.identifier = list(self.record.identifiers[0].split('|'))[0]

    def validateURI(self):
        inTechRegex = r'intechopen.com'
        match = re.search(inTechRegex, self.uri)

        if match:
            return True
        else:
            return False

    def createLinks(self):
        s3Root = self.generateS3Root()

        if self.mediaType == 'application/pdf':
            manifestPath = 'manifests/{}/{}.json'.format(self.source, self.identifier)
            manifestURI = '{}{}'.format(s3Root, manifestPath)

            manifestJSON = self.generateManifest(self.uri, manifestURI)

            htmlURI = self.createHTMLURI()

            # QUESTIONS
            # 1. Do all Intechopen books have PDF links?
            # 2. How do we handle potential errors/add a fallback condition
            # 3. Are PDF links formatted consistently?

            if htmlURI != None:

                return [
                    (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
                    (self.uri, {'reader': False, 'download': True}, 'application/pdf',  None, None),
                    (htmlURI, {'reader': False}, 'text/html', None, None)
                ]
            else:

                return [
                    (manifestURI, {'reader': True}, 'application/webpub+json', (manifestPath, manifestJSON), None),
                    (self.uri, {'reader': False, 'download': True}, 'application/pdf', None, None)
                ]

        elif self.mediaType == 'text/html':
            return []

        return []


    def createHTMLURI(self):
        '''
        Using regular expressions to search for identifier in PDF and parse it into HTML URI
        '''
        
        identRegex = r'intechopen.com\/storage\/books\/([\d]+)'
        match = re.search(identRegex, self.uri)

        if match != None:
            identifier = match.group(1) 

            htmlURI = f'www.intechopen.com/books/{identifier}'

        else:
            htmlURI = None

        return htmlURI

    def generateManifest(self, sourceURI, manifestURI):
        return super().generateManifest(sourceURI, manifestURI)

    def generateS3Root(self):
        return super().generateS3Root()
