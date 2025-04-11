from bs4 import BeautifulSoup
import json
import os
import requests
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

from managers import WebpubManifest
from logger import create_log
from mappings.muse import add_has_part_link

logger = create_log(__name__)


class MUSEManager:
    MUSE_ROOT = 'https://muse.jhu.edu'

    def __init__(self, record, link, media_type):
        self.record = record
        self.muse_id = record.source_id
        self.link = link
        self.media_type = media_type
        self.muse_soup = None

        self.s3_bucket = os.environ['FILE_BUCKET']

        self.pdf_download_url = None
        self.epub_url = None
        self.pdf_webpub_manifest = None

        self.s3_epub_path = None
        self.s3_epub_read_path = None
        self.s3_pdf_read_path = None

    def parse_muse_page(self):
        try:
            muse_html = self.load_muse_page()
        except (ReadTimeout, ConnectionError, HTTPError) as err:
            logger.debug('error retrieving page: {}'.format(err))
            raise MUSEError(
                'Unable to load record from link {}'.format(self.link)
            )

        self.muse_soup = BeautifulSoup(muse_html, features='lxml')

    def identify_readable_versions(self):
        # Extract "TOC" section from Project MUSE detail page
        chapter_table = self.muse_soup.find(id='available_items_list_wrap')

        # If TOC is not present then there are no manifest-able versions
        if not chapter_table:
            logger.info('Cannot extract PDF/ePub version for MUSE #{}'.format(
                self.muse_id
            ))
            return None

        # Look for PDF Download Link
        pdf_button = chapter_table.find(alt='Download PDF')
        if pdf_button:
            pdf_link = pdf_button.parent['href']
            self.pdf_download_url = '{}{}'.format(self.MUSE_ROOT, pdf_link)
            logger.debug(self.pdf_download_url)

        # Look for ePub Link
        epub_button = chapter_table.find(alt='Download EPUB')
        if epub_button:
            epub_relative = epub_button.parent['href']
            self.epub_url = '{}{}'.format(self.MUSE_ROOT, epub_relative)
            logger.debug(self.epub_url)

    def add_readable_links(self):
        if self.pdf_download_url:
            add_has_part_link(
                self.record,
                self.pdf_download_url,
                'application/pdf',
                json.dumps({'download': True, 'reader': False, 'catalog': False})
            )

        if self.epub_url:
            self.s3_epub_path = 'epubs/muse/{}.epub'.format(self.muse_id)
            add_has_part_link(
                self.record,
                self.construct_s3_link(self.s3_epub_path),
                'application/epub+zip',
                json.dumps({'download': True, 'reader': False, 'catalog': False})
            )

            self.epub_read_path = 'epubs/muse/{}/manifest.json'.format(self.muse_id)
            add_has_part_link(
                self.record,
                self.construct_s3_link(self.epub_read_path),
                'application/webpub+json',
                json.dumps({'download': False, 'reader': True, 'catalog': False})
            )

        self.construct_webpub_manifest()
        if self.pdf_webpub_manifest:
            self.s3_pdf_read_path = 'manifests/muse/{}.json'.format(self.muse_id)
            add_has_part_link(
                self.record,
                self.create_manifest_in_s3(self.s3_pdf_read_path),
                'application/webpub+json',
                json.dumps({'reader': True, 'download': False, 'catalog': False})
            )

    def construct_webpub_manifest(self):
        pdf_manifest = WebpubManifest(self.link, self.media_type)
        pdf_manifest.addMetadata(self.record)

        chapter_table = self.muse_soup.find(id='available_items_list_wrap')

        if not chapter_table:
            raise MUSEError('Book {} unavailable'.format(self.muse_id))

        skip_cover = ''

        for card in chapter_table.find_all(class_='card_text'):
            title_item = card.find('li', class_='title') or card.find('div', class_='title')

            pdf_item = card.find(alt='Download PDF')

            title_inner_content = title_item.span or title_item.h3

            if not title_inner_content.a:
                pdf_manifest.addSection(title_inner_content.string, '')
                continue

            if card.parent.get('style') != 'margin-left:30px;':
                pdf_manifest.closeSection()

            if pdf_item:
                #skipCover is iniitally an empty string since the Cover Page doesn't need a query parameter
                pdf_manifest.addChapter(
                    '{}{}{}'.format(
                        self.MUSE_ROOT, pdf_item.parent.get('href'), skip_cover
                    ),
                    title_inner_content.a.string,
                )
            #skipCover is updated with a query parameter to skip the intersitital cover page in every future chapter
            query_param = '?start=2'
            skip_cover = query_param

        pdf_manifest.closeSection()

        if len(pdf_manifest.readingOrder) > 0:
            self.pdf_webpub_manifest = pdf_manifest

    def load_muse_page(self):
        muse_response = requests.get(
            self.link, timeout=15, headers={'User-agent': 'Mozilla/5.0'}
        )

        muse_response.raise_for_status()

        return muse_response.text

    def construct_s3_link(self, bucket_location):
        return 'https://{}.s3.amazonaws.com/{}'.format(
            self.s3_bucket, bucket_location
        )

    def create_manifest_in_s3(self, bucket_location):
        s3_url = self.construct_s3_link(bucket_location)

        self.pdf_webpub_manifest.links.append(
            {'href': s3_url, 'type': 'application/webpub+json', 'rel': 'self'}
        )

        return s3_url

class MUSEError(Exception):
    def __init__(self, message):
        self.message = message
