import pytest

from mappings.hathitrust import HathiMapping


class TestHathingMapping:
    @pytest.fixture
    def testMapping(self, test_constants):
        class TestHathi(HathiMapping):
            def __init__(self):
                self.source = None
                self.mapping = None
                self.constants = test_constants

            def applyMapping(self):
                pass

        return TestHathi()

    @pytest.fixture
    def testRecord_standard(self, mocker):
        return mocker.MagicMock(
            identifiers=["1|hathi", "2|test", "3,4|test"],
            dates=[
                "Test Publisher [1900]|publication_date",
                "2000 [other]|copyright_date",
            ],
            contributors=["contr|test"],
            rights="hathitrust|testLic|testReas|statement|",
            spatial="tst  ",
        )

    @pytest.fixture
    def test_constants(self):
        return {
            "hathitrust": {
                "sourceCodes": {"contr": "Contributor"},
                "rightsValues": {
                    "testLic": {"license": "test", "statement": "Test License"}
                },
                "rightsReasons": {"testReas": "Test Reason"},
            },
            "marc": {"countryCodes": {"tst": "Test Country"}},
        }

    def test_createMapping(self, testMapping):
        recordMapping = testMapping.createMapping()

        assert list(recordMapping.keys()) == [
            "identifiers",
            "rights",
            "is_part_of",
            "title",
            "dates",
            "requires",
            "spatial",
            "languages",
            "contributors",
            "authors",
        ]
        assert recordMapping["title"] == ("{}", 11)
        assert recordMapping["dates"] == [
            ("{}|publication_date", 12),
            ("{}|copyright_date", 16),
        ]

    def test_applyFormatting(self, testMapping, testRecord_standard):
        testMapping.record = testRecord_standard
        testMapping.source = [""] * 24
        testMapping.source[0] = "recordID"

        testMapping.applyFormatting()

        assert testMapping.record.source == "hathitrust"
        assert testMapping.record.source_id == "1|hathi"
        assert testMapping.record.identifiers == [
            "1|hathi",
            "2|test",
            "3|test",
            "4|test",
        ]
        assert testMapping.record.dates == [
            "1900|publication_date",
            "2000 [other]|copyright_date",
        ]
        assert testMapping.record.publisher == ["Test Publisher||"]
        assert testMapping.record.contributors == ["Contributor|test"]
        assert testMapping.record.rights == "hathitrust|test|Test Reason|Test License|"
        assert testMapping.record.has_part == [
            '1|https://babel.hathitrust.org/cgi/pt?id=recordID|hathitrust|text/html|{"reader": false, "download": false, "catalog": false, "embed": true}',
            '1|https://babel.hathitrust.org/cgi/imgsrv/download/pdf?id=recordID|hathitrust|application/pdf|{"reader": false, "download": true, "catalog": false}',
        ]
        assert testMapping.record.spatial == "Test Country"

    def test_applyFormatting_no_pub_date(self, testMapping, testRecord_standard):
        testRecord_standard.dates = ["|publication_date"]
        testMapping.record = testRecord_standard
        testMapping.source = [""] * 24

        testMapping.applyFormatting()

        assert testMapping.record.dates == []
        assert testMapping.record.publisher == ["||"]

    def test_applyFormatting_google(self, testMapping, testRecord_standard):
        testMapping.record = testRecord_standard
        testMapping.source = [""] * 24
        testMapping.source[0] = "recordID"
        testMapping.source[23] = "Google"

        testMapping.applyFormatting()

        assert len(testMapping.record.has_part) == 1
        assert testMapping.record.has_part == [
            '1|https://babel.hathitrust.org/cgi/pt?id=recordID|hathitrust|text/html|{"reader": false, "download": false, "catalog": false, "embed": true}'
        ]
