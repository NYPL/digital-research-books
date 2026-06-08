import pytest
import requests
from services.rights_determiner import determine_rights


@pytest.mark.skip(
    reason="""
    HathiTrust's `volumes/brief` API endpoint started returning inconsistent results.
    Until we can determine the cause and implement a fix, this test will be skipped.
    For reference, the API endpoint being tested is the following URL:

    https://catalog.hathitrust.org/api/volumes/brief/htid/33433116781380.json
    """
)
def test_determine_rights():
    try:
        rights = determine_rights("33433116781380")

        assert rights == "hathitrust|in_copyright||In Copyright|"
    except requests.exceptions.RequestException as e:
        pytest.skip(f"HathiTrust API unavailable: {e}")
