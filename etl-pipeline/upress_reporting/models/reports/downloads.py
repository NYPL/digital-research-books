from models.reports.counter_5_report import Counter5Report


class DownloadsReport(Counter5Report):
    def __init__(self, *args):
        super().__init__(*args)

    def build_report(self, events, reporting_data):
        print("Building downloads report...")

        if len(events) > 0:
            header = self.build_header(
                report_name="NYPL DRB Total Item Requests by Title / Downloads",
                report_description="Downloads of your books from NYPL's Digital Research Books by title.",
                metric_type="Downloads (loading of title contents)",
            )
            columns, final_data = self.aggregate_interaction_events(
                events, reporting_data
            )

            self.write_to_csv(
                file_name=header["Report_Name"],
                header=header,
                column_names=columns,
                data=final_data,
            )

            print("Downloads report generation complete!")
        else:
            print("No download events found in reporting period!")
