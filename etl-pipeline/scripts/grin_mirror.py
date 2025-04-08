# An API interface to https://books.google.com/libraries/NYPL/,
# per https://docs.google.com/document/d/1ayu_djokdss6oCNNYSXtWRyuX7KvtLZ70A99rXy4bR8
# Additional notes - https://docs.google.com/document/d/1aZ5ODEzKP6qX1f4CCcGtlidRvr-Fu8HPA-AEhTwDTyk/edit?usp=sharing

import pickle
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import (
    AuthorizedSession,
    Request,
)
from pdb import set_trace


class GRINClient(object):
    
    # These are the permissions of your Google account the client
    # needs -- basically it needs your access to NYPL's books in GRIN.
    SCOPES = [
        'openid',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile'
    ]
    
    def __init__(self, client_config, token_file, cache_dir):

        # client config is permanent.
        self.client_config = client_config

        # token file is not.
        self.token_file = token_file

        # Data -- books and lists of books -- is kept here.
        self.cache_dir = cache_dir

        # Make sure the cache directories exist.
        for dir in (cache_dir, os.path.join(cache_dir, "books")):
            if not os.path.exists(dir):
                os.makedirs(dir)
        
        self.creds = self.load_creds()
        self.session = AuthorizedSession(self.creds)

    def load_creds(self):
        creds = None
        if os.path.exists(self.token_file):
            creds = pickle.load(open(self.token_file, "rb"))

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(
                    self.client_config, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)

        return creds

    def _url(self, fragment):
        return "https://books.google.com/libraries/NYPL/" + fragment

    def get(self, url, filename, force=False):
        cache_path = os.path.join(self.cache_dir, filename)
        if os.path.exists(cache_path) and not force:
            return open(cache_path, 'rb').read()
        url = self._url(url)
        response = self.session.request("GET", url)
        if response.status_code != 200:
            raise IOError("%s got %s unexpectedly" % (url, response.status_code))
        open(cache_path, "wb").write(response.content)
        return response.content

    def _for_state(self, state, *args, **kwargs):
        "Which books are in the given state?"
        data = self.get("_%s?format=text" % state, "%s.txt" %state, *args, **kwargs)
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

# This 'secret' needs to be exactly as secure as this source code (i.e. not very) so it
# can just go into the file to make things less complicated.
client_config = {"installed":{"client_id":"ID_GOES_HERE.apps.googleusercontent.com","project_id":"project_id","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"CLIENT_SECRET","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}
        
client = GRINClient(client_config, "token.pickle", "cache")
print("Number available: %s" % len(client.available()))
print("Number failed: %s" % len(client.failed()))
converted = client.converted()
print("Downloading %d converted books." % len(converted))
for filename in converted:
    content = client.download(filename)
    print(filename, len(content))
