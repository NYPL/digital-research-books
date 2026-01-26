import os
import json

from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from requests_oauthlib import OAuth2Session

from utils.utils import read_env


class NYPLAPIManager:
    def __init__(self, client_id=None, client_secret=None):
        self.client = None
        self.client_id = client_id or read_env("NYPL_API_CLIENT_ID")
        self.client_secret = client_secret or read_env("NYPL_API_CLIENT_SECRET")
        self.token_url = read_env("NYPL_API_CLIENT_TOKEN_URL")
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

    def post_file_conversion_workflow(self, bucket, mets_key):
        self.generate_access_token()
        self.create_client()

        response = self.client.post(
            self.api_root + "/pdf-pipeline/workflow",
            data=json.dumps({"bucket": bucket, "mets_key": mets_key}),
        )

        return response

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
