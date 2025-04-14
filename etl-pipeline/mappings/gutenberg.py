import json
import re

from mappings.xml import XMLMapping

class GutenbergMapping(XMLMapping):
    def __init__(self, source, namespace, constants, yaml_file):
        super(GutenbergMapping, self).__init__(source, namespace, constants)
        self.mapping = self.createMapping()
        self.yaml_file = yaml_file

    def createMapping(self):
        return {
            'title': ('//dcterms:title/text()', '{0}'),
            'alternative': ('//dcterms:alternative/text()', '{0}'),
            'publisher': [('//dcterms:publisher/text()', '{0}||')],
            'rights': (
                ['//dcterms:rights/text()', '*/cc:license/@rdf:resource'], 'gutenberg|{1}||{0}|'
            ),
            'identifiers': [
                ('//pgterms:ebook/@rdf:about', '{0}|gutenberg'),
                ('//pgterms:marc010/text()', '{0}|lccn'),
                ('//pgterms:marc020/text()', '{0}|isbn'),
                ('//pgterms:marc022/text()', '{0}|issn')
            ],
            'authors': [(
                [
                    '//dcterms:creator/pgterms:agent/pgterms:name/text()',
                    '//dcterms:creator/pgterms:agent/pgterms:birthdate/text()',
                    '//dcterms:creator/pgterms:agent/pgterms:deathdate/text()'
                ],
                '{0} ({1}-{2})|||true')
            ],
            'contributors': [(
                [
                    '//*[starts-with(name(), \'marcrel:\')]/pgterms:agent/pgterms:name/text()',
                    '//*[starts-with(name(), \'marcrel:\')]/pgterms:agent/pgterms:birthdate/text()',
                    '//*[starts-with(name(), \'marcrel:\')]/pgterms:agent/pgterms:deathdate/text()',
                    '//*[starts-with(name(), \'marcrel:\')]'
                ],
                '{0} ({1}-{2})|||{3}')
            ],
            'languages': [('//dcterms:language/rdf:Description/rdf:value/text()', '|{}|')],
            'dates': [('//dcterms:issued/text()', '{0}|issued')],
            'subjects': [(
                [
                    '//dcterms:subject/rdf:Description/dcam:memberOf/@rdf:resource',
                    '//dcterms:subject/rdf:Description/rdf:value/text()'
                ],
                '{0}||{1}')
            ],
            'is_part_of': [('//pgterms:bookshelf/rdf:Description/rdf:value/text()', '{0}||series')]
        }

    def applyFormatting(self):
        self.record.source = 'gutenberg'

        # Parse gutenberg identifier
        gutenbergID = re.search(r'\/([0-9]+)\|', self.record.identifiers[0]).group(1)
        self.record.identifiers[0], self.record.source_id = ('{}|gutenberg'.format(gutenbergID),) * 2

        # Parse subjects for authority data
        for i, subject in enumerate(self.record.subjects):
            subjComponents = subject.split('|')
            authority = subjComponents[0].replace('http://purl.org/dc/terms/', '').lower()
            self.record.subjects[i] = '{}|{}|{}'.format(
                authority, *subjComponents[1:]
            )

        # Parse contributors to set proper roles
        for i, contributor in enumerate(self.record.contributors):
            contribComponents = contributor.split('|')
            lcRelation = self.constants['lc']['relators'].get(contribComponents[-1], 'Contributor')
            self.record.contributors[i] = '{}|{}|{}|{}'.format(
                *contribComponents[:-1], lcRelation
            )

        # Clean up author names if life dates are not present
        for i, author in enumerate(self.record.authors):
            name, *authorComponents = author.split('|')

            if '(-)' not in name: continue

            self.record.authors[i] = '|'.join([name.replace(' (-)', '')] + authorComponents)
