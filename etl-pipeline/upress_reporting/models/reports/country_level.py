from models.reports.counter_5_report import Counter5Report


class CountryLevelReport(Counter5Report):
    def __init__(self, *args):
        super().__init__(*args)

    def build_report(self, events, reporting_data):
        print("Building country-level report...")

        if len(events) > 0:
            header = self.build_header(
                report_name="NYPL DRB Total Item Requests by Title by Country",
                report_description="Usage of your books on NYPL's Digital Research Books by country.",
                metric_type="Views + Downloads",
            )
            columns, final_data = self.aggregate_interaction_events_by_country(
                events, reporting_data
            )

            self.write_to_csv(
                file_name=header["Report_Name"],
                header=header,
                column_names=columns,
                data=final_data,
            )

            print("Country-level report generation complete!")
        else:
            print("No events found in reporting period!")
