import geocoder
import os
import pandas
import re

from models.data.interaction_event import InteractionEvent, InteractionType

IP_REGEX = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
REQUEST_REGEX = r"REST.GET.OBJECT "
TIMESTAMP_REGEX = r"\[.+\]"


class InteractionEventPoller:
    def __init__(
        self,
        date_range,
        reporting_data: pandas.DataFrame,
        file_id_regex,
        bucket_name,
        interaction_type,
    ):
        self.date_range = date_range
        self.reporting_data = reporting_data
        self.file_id_regex = file_id_regex
        self.bucket_name = bucket_name
        self.interaction_type = interaction_type

        self.referrer_url = os.environ.get("REFERRER_URL", None)
        self.get_events(self.bucket_name)

    def get_events(self, bucket_name):
        self.events = self._pull_interaction_events_from_logs(bucket_name)

    def _pull_interaction_events_from_logs(self, bucket_name) -> list[InteractionEvent]:
        events = []
        today = pandas.Timestamp.today()

        for date in self.date_range:
            if date > today:
                print("No logs exist past today's date: ", today.strftime("%b %d, %Y"))
                break

            formatted_date = date.strftime("%Y/%m/%d")
            file_name = f"analytics/upress_reporting/log_files/{bucket_name}/{
                formatted_date
            }/aggregated_log"

            if not os.path.isfile(file_name):
                print(
                    f"There are no logs for {
                        formatted_date
                    }. Attempting to access logs beyond this date..."
                )
                continue

            events_per_day = self._parse_aggregated_log_file(file_name)
            events.extend(events_per_day)

        return events

    def _parse_aggregated_log_file(self, file_name):
        interactions_in_batch = []

        with open(file_name, "r") as aggregated_log_file:
            for line in aggregated_log_file:
                interaction_event = self._match_log_info_with_drb_data(line)

                if interaction_event:
                    interactions_in_batch.append(interaction_event)

        return interactions_in_batch

    def _match_log_info_with_drb_data(self, log_object) -> InteractionEvent | None:
        match_request = re.search(REQUEST_REGEX, log_object)
        match_referrer = re.search(str(self.referrer_url), log_object)

        if not match_request or not match_referrer or "403 AccessDenied" in log_object:
            return None

        match_file_id = re.search(self.file_id_regex, log_object)
        match_time = re.search(TIMESTAMP_REGEX, log_object)
        match_ip = re.search(IP_REGEX, log_object)

        if self.interaction_type == InteractionType.VIEW:
            file_name = match_file_id.group(1).split("/", 1)[1]
        else:
            file_name = match_file_id.group(1).strip().split('"')[0]

        drb_data_match = self.reporting_data.loc[
            self.reporting_data.index.str.contains(file_name)
        ]

        if drb_data_match.empty:
            return None

        match_data = drb_data_match.iloc[0].to_dict()
        match_idx = drb_data_match.index[0]
        self.reporting_data.at[match_idx, "accessed"] = True

        return InteractionEvent(
            country=self._map_ip_to_country(match_ip.group()),
            title=match_data["title"],
            book_id=match_data["book_id"],
            authors=match_data["authors"],
            isbns=match_data["isbns"],
            oclc_numbers=match_data["oclc_numbers"],
            publication_year=match_data["publication_year"],
            disciplines=match_data["disciplines"],
            usage_type=match_data["usage_type"],
            timestamp=match_time[0],
        )

    def _map_ip_to_country(self, ip) -> str | None:
        if re.match(IP_REGEX, ip):
            geocoded_ip = geocoder.ip(ip)
            return geocoded_ip.country
        return None

    def _redact_s3_path(self, path):
        split_path = path.split("/")
        split_path[1] = "NYPL_AWS_ID"
        return "/".join(split_path)


class UnconfiguredEnvironmentError(Exception):
    def __init__(self, message=None):
        self.message = message
