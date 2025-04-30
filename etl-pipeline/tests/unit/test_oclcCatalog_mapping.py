import pytest
import requests
from requests.exceptions import HTTPError

from mappings.oclcCatalog import CatalogMapping


class TestCatalogMapping:
    @pytest.fixture
    def testMapping(self):
        class TestOCLCCatalog(CatalogMapping):
            def __init__(self):
                self.source = None
                self.mapping = None
                self.constants = None
                self.namespace = None

            def applyMapping(self):
                pass

        return TestOCLCCatalog()

    @pytest.fixture
    def testRecord_standard(self, mocker):
        return mocker.MagicMock(
            identifiers=["1|oclc", "2|test"],
            languages=["||820305s1991####nyu###########001#0#eng##|"],
            has_part=["1|uri|test|text/html|{}", "1|uri|bad|text/html|{}"],
        )

    def test_remove_oclc_prefixes(self, testMapping):
        assert testMapping.remove_oclc_prefixes("on48542660") == "48542660"
        assert testMapping.remove_oclc_prefixes("ocm48542660") == "48542660"
        assert testMapping.remove_oclc_prefixes("(OCoLC)on48542660") == "48542660"
        assert testMapping.remove_oclc_prefixes("foo48542660") == "foo48542660"

    def test_createMapping(self, testMapping):
        recordMapping = testMapping.createMapping()

        assert list(recordMapping.keys()) == [
            "title",
            "alternative",
            "authors",
            "publisher",
            "identifiers",
            "contributors",
            "languages",
            "dates",
            "extent",
            "is_part_of",
            "abstract",
            "table_of_contents",
            "subjects",
            "has_part",
        ]
        assert recordMapping["title"] == (
            "//oclc:datafield[@tag='245']/oclc:subfield[@code='a' or @code='b']/text()",
            "{0} {1}",
        )

    def test_applyFormatting(self, testMapping, testRecord_standard, mocker):
        mockParseLink = mocker.patch.object(CatalogMapping, "parseLink")
        mockParseLink.side_effect = ["testLink", None]
        testMapping.record = testRecord_standard

        testMapping.applyFormatting()

        assert testMapping.record.source == "oclcCatalog"
        assert testMapping.record.source_id == "1|oclc"
        assert testMapping.record.frbr_status == "complete"
        assert testMapping.record.languages == ["||eng"]
        assert testMapping.record.has_part == ["testLink"]

    def test_parseLink_gutenberg(self, testMapping, mocker):
        mockRecord = mocker.MagicMock(identifiers=[])
        testMapping.record = mockRecord

        testLink = testMapping.parseLink(
            '1|gutenberg.org/ebooks/1|test|text/html|{"marcInd1": "4"}'
        )
        assert testLink == "1|gutenberg.org/ebooks/1|test|text/html|{}"
        assert testMapping.record.identifiers[0] == "1|gutenberg"

    def test_parseLink_internetarchive_unavailable(self, testMapping, mocker):
        mockCheckIA = mocker.patch.object(CatalogMapping, "checkIAReadability")
        mockCheckIA.return_value = False

        assert (
            testMapping.parseLink(
                '|archive.org/details/1|test|text/html|{"marcInd1": "4"}'
            )
            == None
        )

    def test_parseLink_other(self, testMapping):
        assert (
            testMapping.parseLink('|other.org/1|test|text/html|{"marcInd1": "4"}')
            == None
        )

    def test_parseLink_non_external_url(self, testMapping):
        assert (
            testMapping.parseLink('|other.org/1|test|text/html|{"marcInd1": "0"}')
            == None
        )

    def test_checkIAReadability_true(self, testMapping, mocker):
        mockResp = mocker.MagicMock()
        mockResp.json.return_value = {"metadata": {"access-restricted-item": "false"}}
        mockGet = mocker.patch.object(requests, "get")
        mockGet.return_value = mockResp

        assert testMapping.checkIAReadability("archive.org/1") == True

    def test_checkIAReadability_false(self, testMapping, mocker):
        mockResp = mocker.MagicMock()
        mockResp.json.return_value = {"metadata": {"access-restricted-item": "true"}}
        mockGet = mocker.patch.object(requests, "get")
        mockGet.return_value = mockResp

        assert testMapping.checkIAReadability("archive.org/1") == False

    def test_checkIAReadability_missing(self, testMapping, mocker):
        mockResp = mocker.MagicMock()
        mockResp.json.return_value = {"metadata": {}}
        mockGet = mocker.patch.object(requests, "get")
        mockGet.return_value = mockResp

        assert testMapping.checkIAReadability("archive.org/1") is True

    def test_checkIAReadability_error(self, testMapping, mocker):
        mockResp = mocker.MagicMock()
        mockResp.raise_for_status.side_effect = HTTPError
        mockGet = mocker.patch.object(requests, "get")
        mockGet.return_value = mockResp

        assert testMapping.checkIAReadability("archive.org/1") == False
