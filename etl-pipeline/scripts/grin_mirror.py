# An API interface to https://books.google.com/libraries/NYPL/,
# per https://docs.google.com/document/d/1ayu_djokdss6oCNNYSXtWRyuX7KvtLZ70A99rXy4bR8
# Additional notes - https://docs.google.com/document/d/1aZ5ODEzKP6qX1f4CCcGtlidRvr-Fu8HPA-AEhTwDTyk/edit?usp=sharing

import os.path
from google.auth.transport.requests import (
    AuthorizedSession,
)
from google.oauth2.service_account import Credentials
import json
from services.ssm_service import SSMService
from pdb import set_trace


class GRINClient(object):
    def __init__(self, cache_dir):
        # Data -- books and lists of books -- is kept here.
        self.cache_dir = cache_dir

        # Make sure the cache directories exist.
        for dir in (cache_dir, os.path.join(cache_dir, "books")):
            if not os.path.exists(dir):
                os.makedirs(dir)

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

    def get(self, url, filename, force=False):
        cache_path = os.path.join(self.cache_dir, filename)
        if os.path.exists(cache_path) and not force:
            return open(cache_path, "rb").read()
        url = self._url(url)
        response = self.session.request("GET", url)
        if response.status_code != 200:
            raise IOError("%s got %s unexpectedly" % (url, response.status_code))
        open(cache_path, "wb").write(response.content)
        return response.content

    def _for_state(self, state, *args, **kwargs):
        "Which books are in the given state?"
        data = self.get("_%s?format=text" % state, "%s.txt" % state, *args, **kwargs)
        return data.decode("utf8").strip().split("\n")

    def available(self, *args, **kwargs):
        "Which barcodes are available for conversion?"
        return self._for_state("available", *args, **kwargs)

    def in_process(self, *args, **kwargs):
        "Which barcodes are being converted?"
        return self._for_state("in_process", *args, **kwargs)

    def converted(self, *args, **kwargs):
        "What are the filenames of files that have been converted?"
        return self._for_state("converted", *args, **kwargs)

    def failed(self, *args, **kwargs):
        "Which barcodes failed conversion?"
        return self._for_state("failed", *args, **kwargs)

    def all_books(self, *args, **kwargs):
        "Get barcodes for all books in whatever state."
        return self._for_state("all_books", *args, **kwargs)

    def download(self, filename, *args, **kwargs):
        """Download a book."""
        return self.get(filename, os.path.join("books", filename), *args, **kwargs)

    def please_process(self, barcodes):
        'Ask Google to move some barcodes from the "Available" state to "In-Process".'
        if isinstance(barcodes, list):
            barcodes = "\n".join(barcodes)
        # self.session.request("POST", self._url("_process"), body=barcodes) // signature seems wrong as of 2024
        self.session.request("POST", self._url("_process"), data=barcodes)


client = GRINClient("cache")
print("Number available: %s" % len(client.available()))
print("Number failed: %s" % len(client.failed()))
converted = client.converted()
print("Downloading %d converted books." % len(converted))
for filename in converted:
    content = client.download(filename)
    print(filename, len(content))
