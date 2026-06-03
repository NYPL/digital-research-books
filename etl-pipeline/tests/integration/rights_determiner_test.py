import pytest
import requests
from services.rights_determiner import determine_rights


def test_determine_rights():
    try:
        rights = determine_rights("33433116781380")

        assert rights == "hathitrust|in_copyright||In Copyright|"
    except requests.exceptions.RequestException as e:
        pytest.skip(f"HathiTrust API unavailable: {e}")
