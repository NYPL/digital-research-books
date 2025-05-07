import calendar
import csv
from xml.etree.ElementInclude import include
import pandas
import uuid

from abc import ABC, abstractmethod
from dataclasses import fields, is_dataclass
from datetime import datetime
from models.data.interaction_event import InteractionEvent
from typing import Any, List


class Counter5Report(ABC):
    def __init__(self, publisher, reporting_period):
        self.publisher = publisher
        self.created = datetime.today().strftime("%Y-%m-%d")
        self.reporting_period = reporting_period

    @abstractmethod
    def build_report(self, events, reporting_data):
        return

    def generate_report_id(self):
        return uuid.uuid4()

    def aggregate_interaction_events(self, events, reporting_data):
        columns = [
            "Book Title",
            "Book ID",
            "Authors",
            "ISBN(s)",
            "OCLC Number(s)",
            "Publication Year",
            "Disciplines",
            "Usage Type",
            "Timestamp",
        ]

        accessed_titles_df = self._create_events_df(events, columns)
        accessed_titles_df["Timestamp"] = accessed_titles_df["Timestamp"].apply(
            self._reformat_timestamp_data
        )
        df_grouped_by_id_and_month = accessed_titles_df.groupby(
            ["Book ID", pandas.Grouper(freq="M", key="Timestamp")]
        )
        monthly_columns = []

        for key, group in df_grouped_by_id_and_month:
            month, year = key[1].month, key[1].year
            column_name = "{0} {1}".format(calendar.month_name[int(month)], year)
            f"{month}-{year}"

            if column_name not in accessed_titles_df.columns:
                monthly_columns.append(column_name)
                accessed_titles_df[column_name] = pandas.Series()

            accessed_titles_df.loc[
                accessed_titles_df["Book ID"] == key[0], column_name
            ] = group["Book ID"].count()

        accessed_titles_df.loc[:, monthly_columns] = accessed_titles_df[
            monthly_columns
        ].fillna(0)

        zeroed_out_titles_df = self._format_zeroed_out_titles(
            df=reporting_data, columns=columns, monthly_columns=monthly_columns
        )

        merged_df = pandas.concat(
            [accessed_titles_df, zeroed_out_titles_df], ignore_index=True
        )
        merged_df.drop(columns=["Timestamp"], axis=1, inplace=True)
        monthly_col_idx = merged_df.columns.get_loc(monthly_columns[0])
        merged_df.insert(
            loc=monthly_col_idx,
            column="Reporting Period Total",
            value=merged_df[monthly_columns].sum(axis=1),
        )

        return (merged_df.columns.tolist(), merged_df.to_dict(orient="records"))

    def aggregate_interaction_events_by_country(self, events, reporting_data):
        columns = [
            "Country",
            "Book Title",
            "Book ID",
            "Authors",
            "ISBN(s)",
            "OCLC Number(s)",
            "Publication Year",
            "Disciplines",
            "Usage Type",
            "Timestamp",
        ]

        accessed_titles_df = self._create_events_df(
            events=events, columns=columns, include_country=True
        )
        accessed_titles_df["Timestamp"] = accessed_titles_df["Timestamp"].apply(
            self._reformat_timestamp_data
        )
        df_grouped_by_country_id_and_month = accessed_titles_df.groupby(
            ["Country", "Book ID", pandas.Grouper(freq="M", key="Timestamp")]
        )
        monthly_columns = []

        for key, group in df_grouped_by_country_id_and_month:
            country, book_id, date = key
            month, year = date.month, date.year
            column_name = "{0} {1}".format(calendar.month_name[int(month)], year)
            f"{month}-{year}"

            if column_name not in accessed_titles_df.columns:
                monthly_columns.append(column_name)
                accessed_titles_df[column_name] = pandas.Series()

            accessed_titles_df.loc[
                (accessed_titles_df["Country"] == country)
                & (accessed_titles_df["Book ID"] == book_id),
                column_name,
            ] = group["Book ID"].count()

        zeroed_out_titles_df = self._format_zeroed_out_titles(
            df=reporting_data,
            columns=columns,
            monthly_columns=monthly_columns,
            include_country=True,
        )

        accessed_titles_df.loc[:, monthly_columns] = accessed_titles_df[
            monthly_columns
        ].fillna(0)

        merged_df = pandas.concat(
            [accessed_titles_df, zeroed_out_titles_df], ignore_index=True
        )
        merged_df.drop(columns=["Timestamp"], axis=1, inplace=True)
        monthly_col_idx = merged_df.columns.get_loc(monthly_columns[0])
        merged_df.insert(
            loc=monthly_col_idx,
            column="Reporting Period Total",
            value=merged_df[monthly_columns].sum(axis=1),
        )

        return (merged_df.columns.tolist(), merged_df.to_dict(orient="records"))

    def build_header(self, report_name, report_description, metric_type):
        """TODO: Add further Record.source mappings to publishers as we advance
        in project (ex. University of Louisiana, Lafayette)"""
        publisher_mappings = {"UofMichigan Press": "University of Michigan Press"}

        return {
            "Report_Name": report_name,
            "Report_ID": self.generate_report_id(),
            "Report_Description": report_description,
            "Publisher_Name": publisher_mappings.get(self.publisher, ""),
            "Metric_Type": metric_type,
            "Reporting_Period": self._format_reporting_period_to_string(),
            "Created": self.created,
            "Created_By": "NYPL",
        }

    def write_to_csv(self, file_name, header, column_names, data):
        if "/" in file_name:
            file_name = file_name.replace("/ ", "(") + ")"

        with open(file_name + ".csv", "w") as csv_file:
            writer = csv.writer(csv_file, delimiter="|", quoting=csv.QUOTE_NONE)
            for key, value in header.items():
                writer.writerow([key, value])
            writer.writerow([])
            writer.writerow(column_names)
            for title in data:
                writer.writerow(title.values())

    def _format_zeroed_out_titles(
        self, df, columns, monthly_columns, include_country=False
    ):
        unaccessed_titles = df.loc[df["accessed"] == False]
        recarray = unaccessed_titles.to_records()

        zeroed_out_events = [
            InteractionEvent(
                country=None,
                title=title.title,
                book_id=title.book_id,
                authors=title.authors,
                isbns=title.isbns,
                oclc_numbers=title.oclc_numbers,
                publication_year=title.publication_year,
                disciplines=title.disciplines,
                usage_type=title.usage_type,
                timestamp=None,
            )
            for title in recarray
        ]

        zeroed_out_df = self._create_events_df(
            zeroed_out_events, columns, include_country
        )

        for month in monthly_columns:
            zeroed_out_df[month] = 0

        return zeroed_out_df

    def _format_reporting_period_to_string(self):
        return (
            self.reporting_period[0].strftime("%Y-%m-%d")
            + " to "
            + self.reporting_period[-1].strftime("%Y-%m-%d")
        )

    def _create_events_df(self, events, columns, include_country=False):
        df = pandas.DataFrame(
            [self._format_dataclass_for_df(event, include_country) for event in events]
        )
        df.columns = columns

        if include_country:
            return df.drop_duplicates(subset=["Country", "Book ID"])
        return df.drop_duplicates(subset="Book ID")

    def _format_dataclass_for_df(
        self, dataclass_instance: Any, include_country=False
    ) -> List[Any]:
        if not is_dataclass(dataclass_instance):
            raise ValueError("Provided instance is not a dataclass.")

        csv_row = []

        for field in fields(dataclass_instance):
            if field.name == "country" and not include_country:
                continue
            csv_row.append(getattr(dataclass_instance, field.name))

        return csv_row

    def _reformat_timestamp_data(self, date):
        string_date = str(date).strip("[]").split(":")[0]
        return pandas.to_datetime(string_date)
