from datetime import datetime, timedelta, timezone
import os

from digital_assets import get_stored_file_url
from managers import CoverManager, DBManager, RedisManager, S3Manager
from model import Edition, FileFlags, Link
from model.postgres.edition import EDITION_LINKS
from logger import create_log
from .. import utils

logger = create_log(__name__)


class CoverProcess:
    BATCH_SIZE = 25

    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.db_manager = DBManager()
        self.db_manager.create_session()

        self.redis_manager = RedisManager()
        self.redis_manager.create_client()

        self.s3_manager = S3Manager()
        self.fileBucket = os.environ["FILE_BUCKET"]

        self.run_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

        self.editions_to_update = set()

    def runProcess(self):
        try:
            editions_with_covers_query = self.generate_query()

            self.get_edition_covers(editions_with_covers_query)

            self.db_manager.bulk_save_objects(self.editions_to_update)
        except Exception:
            logger.exception("Failed to run cover process")
        finally:
            self.db_manager.close_connection()

    def generate_query(self):
        base_query = self.db_manager.session.query(Edition)
        sub_query = (
            self.db_manager.session.query(EDITION_LINKS.c.edition_id)
            .join(Link)
            .distinct("edition_id")
            .filter(Link.flags["cover"] == "true")
        )
        filters = [~Edition.id.in_(sub_query)]

        if self.params.process_type != "complete":
            filters.append(
                Edition.date_modified
                >= utils.get_start_datetime(
                    process_type=self.params.process_type,
                    ingest_period=self.params.ingest_period,
                )
            )

        return base_query.filter(*filters)

    def get_edition_covers(self, cover_query):
        for edition in self.db_manager.windowed_query(
            Edition, cover_query, window_size=CoverProcess.BATCH_SIZE
        ):
            cover_manager = self.search_for_cover(edition)

            if cover_manager:
                self.store_cover(cover_manager, edition)

            if (self.run_start_time + timedelta(hours=12)) < datetime.now(
                timezone.utc
            ).replace(tzinfo=None):
                break

    def search_for_cover(self, edition: Edition):
        identifiers = [i for i in self.get_edition_identifiers(edition)]
        manager = CoverManager(identifiers, self.db_manager.session)

        if manager.fetch_cover() is True:
            manager.fetch_cover_file()
            manager.resize_cover_file()

            return manager if manager.cover_content else None

    def get_edition_identifiers(self, edition: Edition):
        for id in edition.identifiers:
            if self.redis_manager.check_or_set_key(
                "sfrCovers",
                id.identifier,
                id.authority,
                expiration_time=60 * 60 * 24 * 30,
            ):
                continue

            yield (id.identifier, id.authority)

    def store_cover(self, manager, edition):
        cover_path = f"covers/{manager.fetcher.SOURCE}/{manager.fetcher.coverID}.{manager.coverFormat.lower()}"
        self.s3_manager.put_object(manager.coverContent, cover_path, self.fileBucket)

        cover_link = Link(
            url=get_stored_file_url(storage_name=self.fileBucket, file_path=cover_path),
            media_type=f"image/{manager.coverFormat.lower()}",
            flags={"cover": True},
        )

        edition.links.append(cover_link)
        self.editions_to_update.add(edition)

        if len(self.editions_to_update) >= CoverProcess.BATCH_SIZE:
            self.db_manager.bulk_save_objects(self.editions_to_update)
            self.editions_to_update.clear()
