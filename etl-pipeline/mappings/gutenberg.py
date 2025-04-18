import json
import os
import mimetypes
import re

import requests

from digital_assets.utils.get_stored_file_url import get_stored_file_url
from logger import create_log
from mappings.xml import XMLMapping
from model import FileFlags, Part, Record

logger = create_log(__name__)


class GutenbergMapping(XMLMapping):
    def __init__(self, source, namespace, constants, yaml_file):
        super(GutenbergMapping, self).__init__(source, namespace, constants)
        self.mapping = self.createMapping()
        self.file_bucket = os.environ["FILE_BUCKET"]
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

        self.add_epub_links(gutenbergID)
        try:
            self.add_cover()
        except Exception as e:
            logger.exception("Cover link generation failed")

    def add_epub_links(self, gutenberg_id):
        self.record.has_part = []
        for i, extension in enumerate(['images', 'noimages']):
            epub_url = f'https://gutenberg.org/ebooks/{gutenberg_id}.epub.{extension}'
            self.record.has_part.append(str(Part(
                index=i+1,
                source=self.record.source.value,
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}.epub",
                ),
                file_type="application/epub+zip",
                flags=str(FileFlags(download=True)),
                source_url=epub_url,
            )))
            self.record.has_part.append(str(Part(
                index=i+1,
                source=self.record.source.value,
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}/META-INF/container.xml",
                ),
                file_type="application/epub+xml",
                flags=str(FileFlags(reader=True)),
            )))
            self.record.has_part.append(str(Part(
                index=i+1,
                source=self.record.source.value,
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}/manifest.json",
                ),
                file_type="application/webpub+json",
                flags=str(FileFlags(reader=True)),
            )))

    def add_cover(self):
        yaml_file = self.yaml_file

        if yaml_file is None:
            return

        for cover_data in yaml_file.get('covers', []):
            if cover_data.get('cover_type') == 'generated':
                continue

            mime_type, _ = mimetypes.guess_type(cover_data.get('image_path'))
            gutenberg_id = yaml_file.get('identifiers', {}).get('gutenberg')

            file_type = re.search(r'(\.[a-zA-Z0-9]+)$', cover_data.get('image_path')).group(1)
            cover_path = 'covers/gutenberg/{}{}'.format(gutenberg_id, file_type)
            cover_url = get_stored_file_url(self.file_bucket, cover_path)
            cover_root = yaml_file.get('url').replace('ebooks', 'files')
            cover_source_url = f"{cover_root}/{cover_data.get('image_path')}"
            response = requests.head(cover_source_url, allow_redirects=True)
            response.raise_for_status()

            self.record.has_part.append(str(Part(
                index=None,
                source=Source.GUTENBERG.value,
                url=cover_url,
                file_type=mime_type,
                flags=str(FileFlags(cover=True)),
                source_url=cover_source_url,
            )))
