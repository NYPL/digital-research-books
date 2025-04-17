import json
import mimetypes
import os
import re
import typing

from digital_assets import get_stored_file_url
from mappings.gutenberg import GutenbergMapping
from managers import DBManager, RabbitMQManager
from model import get_file_message, FileFlags, Part, Record, Source
from logger import create_log
from ..record_buffer import RecordBuffer
from ..record_ingestor import RecordIngestor
from services import GutenbergService
from .. import utils

logger = create_log(__name__)


class GutenbergProcess():
    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.file_bucket = os.environ['FILE_BUCKET']

        self.gutenberg_service = GutenbergService()
        self.record_ingestor = RecordIngestor(source=Source.GUTENBERG.value)

    def runProcess(self) -> int:
        return self.record_ingestor.ingest(self._iter_records())

    def _iter_records(self) -> typing.Generator[Record, None, None]:
        records = self.gutenberg_service.get_records(
            start_timestamp=utils.get_start_datetime(
                process_type=self.params.process_type,
                ingest_period=self.params.ingest_period,
            ),
            offset=self.params.offset,
            limit=self.params.limit,
        )

        count = 0
        for record_mapping in records:
            self.add_epub_links(record_mapping.record)
            try:
                self.add_cover(record_mapping)
            except Exception:
                logger.warning(f'Unable to store cover for {record_mapping.record.source_id}')

            count += 1
            yield record_mapping.record

        return count

    def add_epub_links(self, record: Record):
        gutenberg_id = record.source_id.split("|")[0]
        record.has_part = []
        for i, extension in enumerate(['images', 'noimages']):
            epub_url = f'https://gutenberg.org/ebooks/{gutenberg_id}.epub.{extension}'
            record.has_part.append(str(Part(
                index=i+1,
                source="gutenberg",
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}.epub",
                ),
                file_type="application/epub+zip",
                flags=str(FileFlags(download=True)),
                source_url=epub_url,
            )))
            record.has_part.append(str(Part(
                index=i+1,
                source="gutenberg",
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}/META-INF/container.xml",
                ),
                file_type="application/epub+xml",
                flags=str(FileFlags(reader=True)),
            )))
            record.has_part.append(str(Part(
                index=i+1,
                source="gutenberg",
                url=get_stored_file_url(
                    self.file_bucket,
                    f"epubs/gutenberg/{gutenberg_id}_{extension}/manifest.json",
                ),
                file_type="application/webpub+json",
                flags=str(FileFlags(reader=True)),
            )))

    def add_cover(self, gutenberg_record: GutenbergMapping):
        yaml_file = gutenberg_record.yaml_file

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


            gutenberg_record.record.has_part.append(str(Part(
                index=None,
                source=Source.GUTENBERG.value,
                url=cover_url,
                file_type=mime_type,
                flags=str(FileFlags(cover=True)),
                source_url=cover_source_url,
            )))
