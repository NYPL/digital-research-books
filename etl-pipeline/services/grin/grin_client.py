# An API interface to https://books.google.com/libraries/NYPL/,
# per https://docs.google.com/document/d/1ayu_djokdss6oCNNYSXtWRyuX7KvtLZ70A99rXy4bR8
# Additional notes - https://docs.google.com/document/d/1aZ5ODEzKP6qX1f4CCcGtlidRvr-Fu8HPA-AEhTwDTyk/edit?usp=sharing

import os.path
from datetime import datetime, timedelta
from google.auth.transport.requests import (
    AuthorizedSession,
)
from google.oauth2.service_account import Credentials
import json
from ..ssm_service import SSMService
from pdb import set_trace

BATCH_LIMIT = 1000


class GRINClient(object):
    def __init__(self):
        self.creds = self.load_creds()
        self.session = AuthorizedSession(self.creds)

    def load_creds(self):
        ssm_service = SSMService()
        service_account_file = ssm_service.get_parameter("grin-auth")
        service_account_info = json.loads(service_account_file)

        scopes = [
            "openid",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ]

        creds = Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )
        return creds

    def _url(self, fragment):
        return "https://books.google.com/libraries/NYPL/" + fragment

    def get(self, fragment):
        url = self._url(fragment)
        response = self.session.request("GET", url)
        if response.status_code != 200:
            raise IOError("%s got %s unexpectedly" % (url, response.status_code))
        return response.content

    def convert(self, barcodes):
        # Ask Google to move some barcodes from the "Available" state to "In-Process"
        # Response to process will always be a 200 and the content will a table of Barcodes and corresponding Statuses.
        # For each barcode, a status will be returned of either
        # "Success", "Already being converted" "Other error", "Not allowed to be downloaded"
        response = []

        if len(barcodes) >= BATCH_LIMIT:
            chunked_barcodes = [
                barcodes[i : i + BATCH_LIMIT]
                for i in range(0, len(barcodes), BATCH_LIMIT)
            ]
        else:
            chunked_barcodes = [barcodes]

        for chunk in chunked_barcodes:
            barcodes = "\n".join(chunk)
            raw_response = self.session.request(
                "POST", self._url("_process"), data=barcodes
            )
            sanitized_response = raw_response.content.decode("utf8").split("\n")
            if len(response) > 0:
                # Remove headers if this is not the first request
                response += sanitized_response[1:]
            else:
                response = sanitized_response

        return response

    def acquired_today(self, *args, **kwargs):
        # For GRIN queries, range start is inclusive but the range end is exclusive.
        # This means you must set the upper range to one day after the desired date
        today = datetime.now()
        tomorrow = today + timedelta(1)
        year = today.strftime("%Y")
        month = today.strftime("%m")
        range_start = today.strftime("%Y-%m-%d")
        range_end = tomorrow.strftime("%Y-%m-%d")

        data = self.get(
            "_monthly_report?execute_query=true&year=%s&month=%s&check_in_date_start=%s&check_in_date_end=%s&format=text"
            % (year, month, range_start, range_end)
        )
        data = data.decode("utf8").split("\n")
        return data

    def _for_state(self, state, *args, **kwargs):
        data = self.get("_%s?format=text" % state, *args, **kwargs)
        return data.decode("utf8").strip().split("\n")

    def available_for_conversion(self, *args, **kwargs):
        return self._for_state("available", *args, **kwargs)

    def in_process(self, *args, **kwargs):
        return self._for_state("in_process", *args, **kwargs)

    def converted(self, *args, **kwargs):
        return self._for_state("converted", *args, **kwargs)

    def failed_conversion(self, *args, **kwargs):
        return self._for_state("failed", *args, **kwargs)

    def all_books(self, *args, **kwargs):
        return self._for_state("all_books", *args, **kwargs)

    def download(self, filename, *args, **kwargs):
        return self.get(filename, *args, **kwargs)
