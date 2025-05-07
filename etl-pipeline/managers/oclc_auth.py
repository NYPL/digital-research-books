from datetime import datetime, timezone
import os
import requests
from requests.exceptions import Timeout, ConnectionError
from typing import Optional

from logger import create_log


logger = create_log(__name__)


class OCLCAuthManager:
    _search_token = None
    _search_token_expires_at = None
    _metadata_token = None
    _metadata_token_expires_at = None
    OCLC_SEARCH_AUTH_URL = (
        "https://oauth.oclc.org/token?scope=wcapi&grant_type=client_credentials"
    )
    OCLC_METADATA_AUTH_URL = "https://oauth.oclc.org/token?scope=WorldCatMetadataAPI:view_marc_bib&grant_type=client_credentials"

    TIME_TO_REFRESH_IN_SECONDS = 60

    @classmethod
    def get_search_token(cls):
        OCLC_CLIENT_ID = os.environ.get("OCLC_CLIENT_ID", None)
        OCLC_CLIENT_SECRET = os.environ.get("OCLC_CLIENT_SECRET", None)

        cls._search_token, cls._search_token_expires_at = cls._get_token(
            token=cls._search_token,
            expires_at=cls._search_token_expires_at,
            auth_url=cls.OCLC_SEARCH_AUTH_URL,
            key_id=OCLC_CLIENT_ID,
            key_secret=OCLC_CLIENT_SECRET,
        )

        return cls._search_token

    @classmethod
    def get_metadata_token(cls):
        OCLC_METADATA_ID = os.environ.get("OCLC_METADATA_ID", None)
        OCLC_METADATA_SECRET = os.environ.get("OCLC_METADATA_SECRET", None)

        cls._metadata_token, cls._metadata_token_expires_at = cls._get_token(
            token=cls._metadata_token,
            expires_at=cls._metadata_token_expires_at,
            auth_url=cls.OCLC_METADATA_AUTH_URL,
            key_id=OCLC_METADATA_ID,
            key_secret=OCLC_METADATA_SECRET,
        )

        return cls._metadata_token

    @classmethod
    def _get_token(
        cls,
        token: Optional[str],
        expires_at: Optional[str],
        auth_url: str,
        key_id: str,
        key_secret: str,
    ):
        if token and expires_at:
            expiry_date = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%SZ")
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            time_to_expiry_in_seconds = (expiry_date - now).total_seconds()

            if time_to_expiry_in_seconds > cls.TIME_TO_REFRESH_IN_SECONDS:
                return (token, expires_at)

        try:
            auth_response = requests.post(
                auth_url,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                auth=(key_id, key_secret),
            )
        except (Timeout, ConnectionError):
            logger.warning(f"Failed to retrieve token from {auth_url}")
            return (None, None)

        if auth_response.status_code != 200:
            logger.warning(
                f"OCLC token retrieval from {auth_url} failed with status {auth_response.status_code}"
            )
            return (None, None)

        auth_data = auth_response.json()

        return (auth_data.get("access_token"), auth_data.get("expires_at"))
