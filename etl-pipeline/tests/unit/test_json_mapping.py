import pytest

from mappings.json import JSONMapping


class TestJSONMapping:
    @pytest.fixture
    def testMapping(self):
        return {
            "title": ("title", "{0}"),
            "alt_title": ("alternates", "{0}"),
            "series": (["series_title", "position"], "{0}|{1}"),
            "authors": [("creator", "{0}"), ("author", "{0}")],
            "subjects": [(["heading", "authority"], "{0}|{1}")],
        }

    @pytest.fixture
    def testDocument(self):
        return {
            "title": "Test Title",
            "alternates": ["Alt 1", "Alt 2"],
            "series_title": "Series Title",
            "position": "Volume I",
            "creator": "Tester",
            "author": "Test Author",
            "heading": "Subject",
            "authority": "Auth",
        }

    @pytest.fixture
    def testMapper(self, testMapping, testDocument, mocker):
        class TestJSON(JSONMapping):
            def __init__(self, source, mapping, constants):
                self.source = source
                self.mapping = mapping
                self.constants = constants

            def createMapping(self):
                pass

            def initEmptyRecord(self):
                return mocker.MagicMock(name="mockRecord")

        return TestJSON(testDocument, testMapping, {})

    def test_applyMapping_full_document(self, testMapper, mocker):
        mockFormatting = mocker.patch.object(JSONMapping, "applyFormatting")

        mockFormat = mocker.patch.object(JSONMapping, "formatString")

        mockFormat.side_effect = [
            "title",
            "alt_title",
            "series",
            "creator",
            "author",
            "subjects",
        ]

        testMapper.applyMapping()

        assert testMapper.record.title == "title"
        assert testMapper.record.alt_title == "alt_title"
        assert testMapper.record.series == "series"
        assert testMapper.record.authors == ["creator", "author"]
        assert testMapper.record.subjects == ["subjects"]

        mockFormatting.assert_called_once()
        mockFormat.assert_has_calls(
            [
                mocker.call(("title", "{0}")),
                mocker.call(("alternates", "{0}")),
                mocker.call((["series_title", "position"], "{0}|{1}")),
                mocker.call(("creator", "{0}")),
                mocker.call(("author", "{0}")),
                mocker.call((["heading", "authority"], "{0}|{1}")),
            ]
        )

    def test_formatString_single_field(self, testMapper, testMapping):
        titleStruct = testMapping["title"]

        assert testMapper.formatString(titleStruct) == "Test Title"

    def test_formatString_multi_field(self, testMapper, testMapping):
        subjectStruct = testMapping["subjects"][0]

        assert testMapper.formatString(subjectStruct) == "Subject|Auth"

    def test_formatString_array(self, testMapper, testMapping):
        altStruct = testMapping["alt_title"]

        assert testMapper.formatString(altStruct) == ["Alt 1", "Alt 2"]
