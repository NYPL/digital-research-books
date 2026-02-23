import argparse
import os
import pandas

from datetime import datetime
from helpers import aggregate_logs
from helpers.format_data import format_to_interaction_event
from managers.db import DBManager
from models.data.interaction_event import InteractionType
from models.pollers.interaction_event_poller import InteractionEventPoller
from model.postgres.edition import Edition
from model.postgres.item import Item
from model.postgres.record import Record
from models.reports.country_level import CountryLevelReport
from models.reports.downloads import DownloadsReport
from models.reports.total_usage import TotalUsageReport
from models.reports.views import ViewsReport
from sqlalchemy import CTE, func, literal
from typing import List

VIEW_FILE_ID_REGEX = r"REST.GET.OBJECT manifests/(.*?json)\s"
DOWNLOAD_FILE_ID_REGEX = r"REST.GET.OBJECT titles/(.+pdf\s)"
PUBLISHERS = ["UofMichigan Press"]
DRB_QA_URL = "https://drb-qa.nypl.org"
DRB_PRODUCTION_URL = "https://digital-research-books-beta.nypl.org"


class Counter5Controller:
    def __init__(self, args):
        self.publishers = PUBLISHERS
        self.environment = os.environ.get("ENVIRONMENT", "qa")

        parsed_args = self._parse_args(args)
        if parsed_args is not None:
            self.reporting_period = self._parse_reporting_period(parsed_args)
        else:
            self.reporting_period = (
                f"{datetime.now().year}-01-01 to {datetime.now().year}-01-31"
            )

        self.public_bucket = f"drb-files-${self.environment}-logs"
        self.public_log_path = (
            f"logs/946183545209/us-east-1/drb-files-${self.environment}"
        )
        self.private_bucket = f"drb-files-limited-${self.environment}-logs"
        self.private_log_path = (
            f"logs/946183545209/us-east-1/drb-files-limited-${self.environment}"
        )
        self.referrer_url = (
            DRB_QA_URL if self.environment == "qa" else DRB_PRODUCTION_URL
        )

        self.setup_db_manager()

    def setup_db_manager(self):
        self.db_manager = DBManager()
        self.db_manager.create_session()

    def create_reports(self):
        print("Generating Counter 5 reports...", datetime.now())

        self.pull_aggregated_logs()
        publisher_project_data = self.pull_publisher_project_data()

        for publisher in self.publishers:
            try:
                filtered_publisher_data = self.filter_data_by_publisher(
                    publisher_project_data, publisher
                )
                normalized_publisher_data = self.format_to_reporting_model(
                    filtered_publisher_data
                )

                df = pandas.DataFrame(normalized_publisher_data)
                df.set_index("search_col", inplace=True)

                view_data_poller = InteractionEventPoller(
                    date_range=self.reporting_period,
                    reporting_data=df,
                    file_id_regex=VIEW_FILE_ID_REGEX,
                    bucket_name=self.public_bucket,
                    interaction_type=InteractionType.VIEW,
                )
                download_public_data_poller = InteractionEventPoller(
                    date_range=self.reporting_period,
                    reporting_data=df,
                    file_id_regex=DOWNLOAD_FILE_ID_REGEX,
                    bucket_name=self.public_bucket,
                    interaction_type=InteractionType.DOWNLOAD,
                )
                download_private_data_poller = InteractionEventPoller(
                    date_range=self.reporting_period,
                    reporting_data=df,
                    file_id_regex=DOWNLOAD_FILE_ID_REGEX,
                    bucket_name=self.private_bucket,
                    interaction_type=InteractionType.DOWNLOAD,
                )

                merged_downloads_reporting_data = pandas.concat(
                    [
                        download_public_data_poller.reporting_data,
                        download_private_data_poller.reporting_data,
                    ],
                    ignore_index=True,
                )
                downloads_report = DownloadsReport(publisher, self.reporting_period)
                downloads_report.build_report(
                    download_public_data_poller.events
                    + download_private_data_poller.events,
                    merged_downloads_reporting_data,
                )

                views_report = ViewsReport(publisher, self.reporting_period)
                views_report.build_report(
                    view_data_poller.events, view_data_poller.reporting_data
                )

                merged_reporting_data = pandas.concat(
                    [merged_downloads_reporting_data, view_data_poller.reporting_data],
                    ignore_index=True,
                )
                merged_reporting_data = merged_reporting_data.sort_values(
                    ["accessed"]
                ).drop_duplicates(subset=["book_id"], keep="last")

                country_level_report = CountryLevelReport(
                    publisher, self.reporting_period
                )
                country_level_report.build_report(
                    view_data_poller.events
                    + download_public_data_poller.events
                    + download_private_data_poller.events,
                    merged_reporting_data,
                )

                total_usage_report = TotalUsageReport(publisher, self.reporting_period)
                total_usage_report.build_report(
                    view_data_poller.events
                    + download_public_data_poller.events
                    + download_private_data_poller.events,
                    merged_reporting_data,
                )

            except Exception as e:
                print("Terminating process. Exception encountered: ", e)
                raise e

        self.db_manager.close_connection()
        print("Done building Counter 5 reports! ", datetime.now())

    def pull_aggregated_logs(self):
        aggregate_logs.aggregate_logs_in_period(
            date_range=self.reporting_period,
            s3_bucket=self.public_bucket,
            s3_path=self.public_log_path,
            regex=VIEW_FILE_ID_REGEX,
            referrer_url=self.referrer_url,
        )

        aggregate_logs.aggregate_logs_in_period(
            date_range=self.reporting_period,
            s3_bucket=self.public_bucket,
            s3_path=self.public_log_path,
            regex=DOWNLOAD_FILE_ID_REGEX,
            referrer_url=self.referrer_url,
        )

        aggregate_logs.aggregate_logs_in_period(
            date_range=self.reporting_period,
            s3_bucket=self.private_bucket,
            s3_path=self.private_log_path,
            regex=DOWNLOAD_FILE_ID_REGEX,
            referrer_url=self.referrer_url,
        )

    def pull_publisher_project_data(self) -> CTE:
        publisher_project_records = (
            self.db_manager.session.query(Record)
            .filter(Record.publisher_project_source.is_not(None))
            .cte("temp_records")
        )
        publisher_project_items = (
            self.db_manager.session.query(Item.edition_id)
            .filter(Item.publisher_project_source.is_not(None))
            .cte("temp_items")
        )
        publisher_project_editions = (
            self.db_manager.session.query(
                Edition.dates,
                Edition.id,
                Edition.title,
                func.unnest(Edition.dcdw_uuids).label("uuid"),
            )
            .join(
                publisher_project_items,
                publisher_project_items.c.edition_id == Edition.id,
            )
            .cte("temp_editions")
        )
        merged_publisher_project_data = (
            self.db_manager.session.query(
                publisher_project_editions.c.dates,
                publisher_project_editions.c.id,
                publisher_project_editions.c.title,
                publisher_project_records.c.authors,
                publisher_project_records.c.has_part,
                publisher_project_records.c.identifiers,
                publisher_project_records.c.publisher_project_source,
                publisher_project_records.c.subjects,
                literal(False).label("accessed"),
            )
            .join(
                publisher_project_editions,
                publisher_project_records.c.uuid == publisher_project_editions.c.uuid,
            )
            .cte("merged_publisher_project_data")
        )

        return merged_publisher_project_data

    def filter_data_by_publisher(self, cte: CTE, publisher: str):
        return (
            self.db_manager.session.query(cte)
            .filter(cte.c.publisher_project_source == publisher)
            .all()
        )

    def format_to_reporting_model(self, filtered_publisher_data):
        return [
            format_to_interaction_event(data, self.referrer_url)
            for data in filtered_publisher_data
        ]

    def _parse_args(self, args):
        parser = argparse.ArgumentParser(prog="Counter 5 Report Generator")

        parser.add_argument(
            "--start", "-s", help="starting date in the format yyyy-mm-dd"
        )
        parser.add_argument("-e", "--end", help="end date in the format yyyy-mm-dd")
        parser.add_argument("-y", "--year", help="fiscal year, ex. 2024")
        parser.add_argument("-q", "--quarter", help="fiscal quarter, ex. Q1")

        return parser.parse_args(args)

    def _parse_reporting_period(self, parsed_args, freq="D"):
        if parsed_args.start and parsed_args.end:
            start, end = parsed_args.start, parsed_args.end
            return pandas.date_range(start=start, end=end, freq=freq)

        if parsed_args.year and parsed_args.quarter:
            period = pandas.Period(parsed_args.year + parsed_args.quarter, freq="Q-JUN")
            return pandas.date_range(
                start=period.start_time, end=period.end_time, freq=freq
            )
