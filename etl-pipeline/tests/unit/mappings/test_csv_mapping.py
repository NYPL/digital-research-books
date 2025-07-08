import pytest

from mappings.csv import CSVMapping


class TestCSVMapping:
    @pytest.fixture
    def testMapping(self):
        return {
            "title": ("{}: {}", 0, 1),
            "uuid": ("{}", 2),
            "authors": [("{}|{}|{}|author", 3, 4, 5)],
            "identifiers": [("{}|isbn", 6), ("{}|oclc", 7)],
        }

    @pytest.fixture
    def testSource_full(self):
        return [
            "Title",
            "Sub Title",
            "uuid1",
            "Author 1",
            "lcnaf",
            "viaf",
            "isbn1",
            "oclc1",
        ]

    @pytest.fixture
    def testSource_partial(self):
        return ["Title2", "", "uuid2", "Author 2", "", "", "", "oclc2"]

    @pytest.fixture
    def testSource_missing(self):
        return ["Title3", "Sub 3", "uuid3", "", "", "", "isbn2"]

    @pytest.fixture
    def testMapper(self, testMapping, mocker):
        class TestCSV(CSVMapping):
            def __init__(self, source, mapping, constants):
                self.source = source
                self.mapping = mapping
                self.constants = constants

            def createMapping(self):
                pass

            def initEmptyRecord(self):
                return mocker.MagicMock(name="mockRecord")

        return TestCSV(None, testMapping, {})

    def test_applyMapping_full_row(self, testMapper, testSource_full, mocker):
        mockFormatting = mocker.patch.object(CSVMapping, "applyFormatting")

        testMapper.source = testSource_full
        testMapper.applyMapping()

        assert testMapper.record.title == "Title: Sub Title"
        assert testMapper.record.uuid == "uuid1"
        assert testMapper.record.authors == ["Author 1|lcnaf|viaf|author"]
        assert testMapper.record.identifiers == ["isbn1|isbn", "oclc1|oclc"]

    def test_applyMapping_partial_row(self, testMapper, testSource_partial, mocker):
        mockFormatting = mocker.patch.object(CSVMapping, "applyFormatting")

        testMapper.source = testSource_partial
        testMapper.applyMapping()

        assert testMapper.record.title == "Title2: "
        assert testMapper.record.uuid == "uuid2"
        assert testMapper.record.authors == ["Author 2|||author"]
        assert testMapper.record.identifiers == ["oclc2|oclc"]

    def test_applyMapping_missing_row(self, testMapper, testSource_missing, mocker):
        mockFormatting = mocker.patch.object(CSVMapping, "applyFormatting")

        testMapper.source = testSource_missing
        testMapper.applyMapping()

        assert testMapper.record.title == "Title3: Sub 3"
        assert testMapper.record.uuid == "uuid3"
        assert testMapper.record.authors == ["|||author"]
        assert testMapper.record.identifiers == []
