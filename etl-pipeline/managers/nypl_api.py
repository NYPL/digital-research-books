from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
import os
from requests_oauthlib import OAuth2Session


class NYPLAPIManager:
    def __init__(self, client_id=None, client_secret=None):
        super(NYPLAPIManager, self).__init__()
        self.client = None
        self.client_id = client_id or os.environ.get("NYPL_API_CLIENT_ID", None)
        self.client_secret = client_secret or os.environ.get(
            "NYPL_API_CLIENT_SECRET", None
        )
        self.token_url = os.environ.get("NYPL_API_CLIENT_TOKEN_URL", None)
        self.api_root = "https://platform.nypl.org/api/v0.1"
        self.token = None

    def generate_access_token(self):
        client = BackendApplicationClient(self.client_id)
        oauth = OAuth2Session(client=client)
        self.token = oauth.fetch_token(
            token_url=self.token_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def create_client(self):
        self.client = OAuth2Session(self.client_id, token=self.token)

    def query_api(self, request_path):
        if not self.client:
            self.create_client()

        try:
            return self.client.get(
                "{}/{}".format(self.api_root, request_path), timeout=15
            ).json()
        except TokenExpiredError:
            self.generate_access_token()
            self.client = None

            return self.query_api(request_path)
        except TimeoutError:
            return {}
