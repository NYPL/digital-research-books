import pytest

from mappings.nypl import NYPLMapping


class TestNYPLMapping:
    @pytest.fixture
    def testMapping(self, test_constants):
        class TestNYPL(NYPLMapping):
            def __init__(self):
                self.source = None
                self.mapping = None
                self.constants = test_constants

        return TestNYPL()

    @pytest.fixture
    def test_constants(self):
        return {"lc": {"relators": {"tst": "Tester"}}}

    @pytest.fixture
    def testSource(self):
        return {
            "var_fields": [
                {
                    "marcTag": 245,
                    "ind1": "1",
                    "ind2": "2",
                    "content": "Title Statement",
                    "subfields": [],
                },
                {
                    "marcTag": 856,
                    "ind1": "a",
                    "ind2": "z",
                    "content": "",
                    "subfields": [{"tag": "a", "content": "testLink"}],
                },
            ]
        }

    @pytest.fixture
    def testRecord_standard(self, mocker):
        return mocker.MagicMock(
            identifiers=["1|nypl", ["2|test"], "3|scn(OCoLC)"],
            subjects=["top -- middle -- -- bottom|1|lcsh"],
            contributors=["Contributor|1234|n9876|tst"],
            has_part=[],
            coverage=[],
        )

    @pytest.fixture
    def testBibItems(self):
        return [
            {"id": "1", "location": {"name": "Test", "code": "tst"}},
            {"id": "2", "location": {"name": "Other", "code": "otr"}},
            {"id": "3", "location": {"name": "Error", "code": "err"}},
        ]

    @pytest.fixture
    def testLocations(self):
        return {"tst": {"requestable": True}, "otr": {"requestable": False}}

    def test_createMapping(self, testMapping):
        recordMapping = testMapping.createMapping()

        assert list(recordMapping.keys()) == [
            "title",
            "alternative",
            "authors",
            "languages",
            "dates",
            "spatial",
            "identifiers",
            "publisher",
            "contributors",
            "has_version",
            "extent",
            "is_part_of",
            "abstract",
            "table_of_contents",
            "subjects",
            "has_part",
        ]
        assert recordMapping["title"] == ("title", "{0}")

    def test_parseVarFields(self, testMapping, testSource):
        testMapping.source = testSource

        testMapping.parseVarFields()

        assert testMapping.source[245]["content"] == "Title Statement"
        assert testMapping.source[856]["ind1"] == "a"
        assert testMapping.source[856]["a"] == "testLink"

    def test_applyFormatting(
        self, testMapping, testRecord_standard, testBibItems, testLocations
    ):
        testMapping.record = testRecord_standard
        testMapping.locationCodes = testLocations
        testMapping.bibItems = testBibItems
        testMapping.source = {"id": "1"}

        testMapping.applyFormatting()

        assert testMapping.record.source == "nypl"
        assert testMapping.record.source_id == "1|nypl"
        assert set(testMapping.record.identifiers) == set(
            ["1|nypl", "2|test", "3|oclc"]
        )
        assert testMapping.record.subjects == ["top -- middle -- bottom|1|lcsh"]
        assert testMapping.record.contributors == ["Contributor|1234|n9876|Tester"]
        assert testMapping.record.coverage == ["tst|Test|2"]
        assert testMapping.record.has_part == [
            '1|https://www.nypl.org/research/collections/shared-collection-catalog/bib/b1|nypl|application/html+catalog|{"catalog": true, "download": false, "reader": false}',
            '2|http://www.nypl.org/research/collections/shared-collection-catalog/hold/request/b1-i1|nypl|application/x.html+edd|{"edd": true, "catalog": false, "download": false, "reader": false, "nypl_login": true}',
        ]
