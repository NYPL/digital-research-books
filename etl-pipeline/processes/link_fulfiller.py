import os
import copy
import json

from logger import create_log
from managers import DBManager, S3Manager
from model import Link, Record


logger = create_log(__name__)


class LinkFulfiller:
    def __init__(self, db_manager: DBManager):
        self.db_manager = db_manager

        self.s3_manager = S3Manager()

        self.api_url = os.environ.get("DRB_API_URL")

    def fulfill_records_links(self, records: list[Record]):
        for record in records:
            if any(
                json.loads(part.flags).get("fulfill_limited_access")
                for part in record.parts
            ):
                self._fulfill_manifest(record)

    def _fulfill_manifest(self, record: Record):
        manifest_part = next(
            filter(
                lambda part: part.file_type == "application/webpub+json", record.parts
            ),
            None,
        )

        if manifest_part is None:
            logger.warning(f"No manifest found for record: {record}")
            return

        manifest_file = self.s3_manager.client.get_object(
            Bucket=manifest_part.file_bucket, Key=manifest_part.file_key
        )

        manifest = json.loads(manifest_file["Body"].read().decode("utf-8"))
        fulfilled_manifest = self._update_manifest_links(manifest)

        if manifest == fulfilled_manifest:
            logger.info(
                f"Manifest links have already been fulfilled for record: {record}"
            )
            return

        self.s3_manager.client.put_object(
            Bucket=manifest_part.file_bucket,
            Key=manifest_part.file_key,
            Body=json.dumps(fulfilled_manifest, ensure_ascii=False),
            ACL="public-read",
            ContentType="application/json",
        )

    def _update_manifest_links(self, manifest_json: dict):
        fulfilled_manifest = copy.deepcopy(manifest_json)

        for manifest_sections in ["links", "readingOrder", "resources", "toc"]:
            for manifest_link in fulfilled_manifest.get(manifest_sections, []):
                manifest_link["href"] = self._fulfill_link(manifest_link)

        return fulfilled_manifest

    def _fulfill_link(self, manifest_link):
        if manifest_link.get("type") in {
            "application/pdf",
            "application/epub+zip",
            "application/epub+xml",
        } or ("pdf" in manifest_link["href"] or "epub" in manifest_link["href"]):
            link = (
                self.db_manager.session.query(Link)
                .filter(Link.url == manifest_link["href"].replace("https://", ""))
                .first()
            )

            return (
                f"{self.api_url}/fulfill/{link.id}" if link else manifest_link["href"]
            )

        return manifest_link["href"]
