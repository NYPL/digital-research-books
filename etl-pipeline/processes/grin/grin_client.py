# An API interface to https://books.google.com/libraries/NYPL/

from datetime import datetime, timedelta
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials
import json
from requests import HTTPError
from services.ssm_service import SSMService
from logger import create_log

BATCH_LIMIT = 100

logger = create_log(__name__)


class GRINClient(object):
    def __init__(self):
        self.creds = self.load_creds()
        self.session = AuthorizedSession(self.creds)

    def load_creds(self):
        ssm_service = SSMService()
        service_account_file = ssm_service.get_parameter(
            "grin-auth", raise_on_error=True
        )
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

    def get(self, fragment, stream=False, **kwargs):
        url = self._url(fragment)

        response = self.session.request("GET", url, stream=stream, **kwargs)

        if response.status_code != 200:
            raise IOError("%s got %s unexpectedly" % (url, response.status_code))

        return response if stream else response.content

    def convert(self, barcodes):
        # Ask Google to move some barcodes from the "Available" state to "In-Process"
        # Response to process will always be a 200 and the content will a table of Barcodes and corresponding Statuses.
        # For each barcode, a status will be returned of either:
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

            try:
                raw_response.raise_for_status()
            except HTTPError:
                logger.exception(f"Failed to convert barcodes")
                return response

            sanitized_response = raw_response.content.decode("utf8").split("\n")
            if len(response) > 0:
                # Remove headers if this is not the first request
                response += sanitized_response[1:]
            else:
                response = sanitized_response

        return response

    def download(self, filename, *args, **kwargs):
        return self.get(filename, *args, **kwargs)

    def get_new_barcodes(self, *args, **kwargs):
        today = datetime.now()
        yesterday = today - timedelta(1)
        range_start = yesterday.strftime("%Y-%m-%d")
        range_end = today.strftime("%Y-%m-%d")
        barcodes = self.get(
            "_all_books?execute_query=true&format=text&last_scan_date_start=%s&last_scan_date_end=%s"
            % (range_start, range_end)
        )
        barcodes = barcodes.decode("utf8").strip().split("\n")
        return barcodes

    def _for_state(self, state, *args, **kwargs):
        data = self.get("_%s?format=text" % state, *args, **kwargs)
        return data.decode("utf8").strip().split("\n")

    def available_for_conversion(self, *args, **kwargs):
        return self._for_state("available", *args, **kwargs)

    def in_process(self, *args, **kwargs):
        return self._for_state("in_process", *args, **kwargs)

    def converted_filenames(self, *args, **kwargs):
        return self._for_state("converted", *args, **kwargs)

    def failed_conversion(self, *args, **kwargs):
        return self._for_state("failed", *args, **kwargs)

    def all_books(self, *args, **kwargs):
        return self._for_state("all_books", *args, **kwargs)
