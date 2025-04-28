import json
import re
import requests
from requests.exceptions import ReadTimeout, HTTPError

from mappings.xml import XMLMapping
from model import RecordState
from logger import create_log

logger = create_log(__name__)


class CatalogMapping(XMLMapping):
    EBOOK_REGEX = {
        'gutenberg': r'gutenberg.org\/ebooks\/([0-9]+)',
        'internetarchive': r'archive.org\/details\/([a-z0-9]+)$',
        'hathitrust': r'catalog.hathitrust.org\/api\/volumes\/([a-z]{3,6}\/[a-zA-Z0-9]+)\.html'
    }

    OCLC_PREFIXES = ['(OCoLC)', 'on', 'ocn', 'ocm']

    def __init__(self, source, namespace, constants):
        super(CatalogMapping, self).__init__(source, namespace, constants)
        self.mapping = self.createMapping()

    def remove_oclc_prefixes(self, oclc_id):
        for prefix in self.OCLC_PREFIXES:
            oclc_id = oclc_id.removeprefix(prefix)
        return oclc_id


    def createMapping(self):
        return {
            'title': ('//oclc:datafield[@tag=\'245\']/oclc:subfield[@code=\'a\' or @code=\'b\']/text()', '{0} {1}'),
            'alternative': [
                ('//oclc:datafield[@tag=\'210\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
                ('//oclc:datafield[@tag=\'222\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
                ('//oclc:datafield[@tag=\'242\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
                ('//oclc:datafield[@tag=\'246\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
                ('//oclc:datafield[@tag=\'247\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
            ],
            'authors': [
                ([
                    '//oclc:datafield[@tag=\'100\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'100\']/oclc:subfield[@code=\'d\']/text()',
                ], '{0} ({1})||true'),
                ([
                    '//oclc:datafield[@tag=\'110\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'110\']/oclc:subfield[@code=\'b\']/text()',
                ], '{0} {1}||true')
            ],
            'publisher': [('//oclc:datafield[@tag=\'260\']/oclc:subfield[@code=\'b\']/text()', '{0}||')],
            'identifiers': [
                ('//oclc:controlfield[@tag=\'001\']/text()', '{0}|oclc'),
                ('//oclc:datafield[@tag=\'010\']/oclc:subfield[@code=\'a\']/text()', '{0}|lccn'),
                ('//oclc:datafield[@tag=\'020\']/oclc:subfield[@code=\'a\']/text()', '{0}|isbn'),
                ('//oclc:datafield[@tag=\'022\']/oclc:subfield[@code=\'a\']/text()', '{0}|issn'),
                ('//oclc:datafield[@tag=\'050\']/oclc:subfield[@code=\'a\']/text()', '{0}|lcc'),
                ('//oclc:datafield[@tag=\'082\']/oclc:subfield[@code=\'a\']/text()', '{0}|ddc'),
                ('//oclc:datafield[@tag=\'010\']/oclc:subfield[@code=\'z\']/text()', '{0}|lccn'),
                ('//oclc:datafield[@tag=\'020\']/oclc:subfield[@code=\'z\']/text()', '{0}|isbn'),
                ('//oclc:datafield[@tag=\'022\']/oclc:subfield[@code=\'z\']/text()', '{0}|issn'),
                ('//oclc:datafield[@tag=\'050\']/oclc:subfield[@code=\'z\']/text()', '{0}|lcc'),
                ('//oclc:datafield[@tag=\'082\']/oclc:subfield[@code=\'z\']/text()', '{0}|dcc'),
            ],
            'contributors': [
                ('//oclc:datafield[@tag=\'260\']/oclc:subfield[@code=\'f\']/text()', '{0}|||manufacturer'),
                ('//oclc:datafield[@tag=\'700\']/oclc:subfield[@code=\'f\']/text()', '{0}|||contributor'),
                ('//oclc:datafield[@tag=\'710\']/oclc:subfield[@code=\'f\']/text()', '{0}|||contributor'),
                ('//oclc:datafield[@tag=\'711\']/oclc:subfield[@code=\'f\']/text()', '{0}|||contributor'),
            ],
            'languages': [('//oclc:controlfield[@tag=\'008\']/text()', '||{}')],
            'dates': [
                ('//oclc:datafield[@tag=\'260\']/oclc:subfield[@code=\'c\']/text()', '{0}|publication_date'),
                ('//oclc:datafield[@tag=\'264\']/oclc:subfield[@cod=\'c\']/text()', '{0}|copyright_date')
            ],
            'extent': (
                [
                    '//oclc:datafield[@tag=\'300\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'300\']/oclc:subfield[@code=\'b\']/text()',
                    '//oclc:datafield[@tag=\'300\']/oclc:subfield[@code=\'c\']/text()'
                ],
                '{0}{1}{b}'),
            'is_part_of': [
                (
                    [
                        '//oclc:datafield[@tag=\'440\']/oclc:subfield[@code=\'a\']/text()',
                        '//oclc:datafield[@tag=\'440\']/oclc:subfield[@code=\'v\']/text()'
                    ],
                    '{0}|{1}|volume'
                ),
                (
                    [
                        '//oclc:datafield[@tag=\'490\']/oclc:subfield[@code=\'a\']/text()',
                        '//oclc:datafield[@tag=\'490\']/oclc:subfield[@code=\'v\']/text()'
                    ],
                    '{0}|{1}|volume'
                )
            ],
            'abstract': ('//oclc:datafield[@tag=\'520\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
            'table_of_contents': ('//oclc:datafield[@tag=\'505\']/oclc:subfield[@code=\'a\']/text()', '{0}'),
            'subjects': [(
                [
                    '//oclc:datafield[@tag=\'600\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'600\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'600\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'610\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'610\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'610\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'611\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'611\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'611\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'630\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'630\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'630\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'650\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'650\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'650\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'651\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'651\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'651\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'655\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'655\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'655\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'656\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'656\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'655\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),
                ([
                    '//oclc:datafield[@tag=\'690\']/oclc:subfield[@code=\'a\']/text()',
                    '//oclc:datafield[@tag=\'690\']/oclc:subfield[@code=\'2\']/text()',
                    '//oclc:datafield[@tag=\'690\']/oclc:subfield[@code=\'0\']/text()',
                ],
                '{0}|{1}|{2}'),

            ],
            'has_part': [(
                [
                    '//oclc:datafield[@tag=\'856\']/oclc:subfield[@code=\'u\']/text()',
                    '//oclc:datafield[@tag=\'856\']/@ind1'
                ],
                '1|{0}|oclc|text/html|{{"download": false, "reader": false, "catalog": false, "embed": true, "marcInd1": "{1}"}}'
            )]
        }

    def applyFormatting(self):
        self.record.source = 'oclcCatalog'
        self.record.identifiers[0] = self.remove_oclc_prefixes(self.record.identifiers[0])

        oclc_number = int(self.record.identifiers[0].split('|')[0])

        self.record.source_id = f'{oclc_number}|oclc'
        self.record.identifiers[0] = self.record.source_id

        # TODO: deprecate frbr_status
        self.record.frbr_status = 'complete'
        self.record.state = RecordState.EMBELLISHED.value

        _, _, lang_3, *_ = tuple(self.record.languages[0].split('|'))
        self.record.languages = [('||{}'.format(lang_3[35:38]))]

        self.record.has_part = self.record.has_part[:10]

        self.record.has_part = list(filter(None, [
            self.parseLink(p) for p in self.record.has_part
        ]))

    def parseLink(self, part):
        partNo, partLink, partSource, partFormat, partFlags = part.split('|')

        partDict = json.loads(partFlags)

        if partDict.get('marcInd1') != '4' or partLink == '':
            return None

        for source, sourceRegex in self.EBOOK_REGEX.items():
            sourceMatch = re.search(sourceRegex, partLink)

            if sourceMatch:
                sourceID = sourceMatch.group(1)

                if source == 'internetarchive':
                    if self.checkIAReadability(partLink) is False:
                        return None

                    # TODO
                    # 1) Parse HathiTrust Links
                    # 2) Parse Project Gutenberg Links
                    logger.debug('Adding IA Link {}'.format(partLink))

                self.record.identifiers.append('{}|{}'.format(
                    sourceID, source
                ))

                del partDict['marcInd1']
                return '|'.join([
                    partNo,
                    partLink,
                    partSource,
                    partFormat,
                    json.dumps(partDict)
                ])

    def checkIAReadability(self, iaURL):
        metadataURL = iaURL.replace('details', 'metadata')

        try:
            metadataResp = requests.get(metadataURL, timeout=5)
            metadataResp.raise_for_status()
        except Exception:
            logger.exception(f'Failed to check Internet Archive readability for {iaURL}')
            return False

        iaData = metadataResp.json()
        iaAccessStatus = iaData['metadata']\
            .get('access-restricted-item', 'false')

        return iaAccessStatus == 'false'
