from lxml import etree
import pytest

from mappings.sql import SQLMapping


class TestSQLMapping:
    @pytest.fixture
    def testMapper(self):
        class TestSQL(SQLMapping):
            def __init__(self):
                super().__init__(None, None)

            def createMapping(self):
                pass

        return TestSQL()

    @pytest.fixture
    def testMapping(self):
        return {
            "title": ("title", "{0}"),
            "alt_titles": ("alt_titles", "{0}"),
            "language": ("language", "{name}|{code}"),
            "contributors": [("printer", "{0}"), ("manufacturer", "{0}")],
        }

    @pytest.fixture
    def testDoc(self):
        return {
            "title": "Test Title",
            "alt_titles": ["Alt 1", "Alt 2"],
            "language": {"name": "Test", "code": "tst"},
            "printer": "Test Printer",
            "manufacturer": "Test Manufacturer",
        }

    def test_applyMapping(self, testMapper, testMapping, testDoc, mocker):
        mapperMocks = mocker.patch.multiple(
            SQLMapping,
            initEmptyRecord=mocker.DEFAULT,
            getFieldData=mocker.DEFAULT,
            applyFormatting=mocker.DEFAULT,
        )
        mapperMocks["initEmptyRecord"].return_value = mocker.MagicMock()
        mapperMocks["getFieldData"].side_effect = [
            "Test Title",
            ["Alt 1", "Alt 2"],
            "Test|test",
            "Test Printer",
            "Test Manufacturer",
        ]

        testMapper.mapping = testMapping
        testMapper.source = testDoc

        testMapper.applyMapping()

        assert testMapper.record.title == "Test Title"
        assert testMapper.record.alt_titles == ["Alt 1", "Alt 2"]
        assert testMapper.record.language == "Test|test"
        assert testMapper.record.contributors == ["Test Printer", "Test Manufacturer"]
        mapperMocks["applyFormatting"].assert_called_once

    def test_getFieldData_string(self, testMapper, testMapping, testDoc):
        testData = testMapper.getFieldData(testDoc["title"], testMapping["title"][1])

        assert testData == "Test Title"

    def test_getFieldData_list(self, testMapper, testMapping, testDoc):
        testData = testMapper.getFieldData(
            testDoc["alt_titles"], testMapping["alt_titles"][1]
        )

        assert testData == ["Alt 1", "Alt 2"]

    def test_getFieldData_dict(self, testMapper, testMapping, testDoc):
        testData = testMapper.getFieldData(
            testDoc["language"], testMapping["language"][1]
        )

        assert testData == "Test|tst"

    def test_getFieldData_empty_string(self, testMapper, testMapping, testDoc):
        testData = testMapper.getFieldData("", testMapping["title"][1])

        assert testData is None

    def test_getFieldData_empty_dict(self, testMapper, testMapping, testDoc):
        testData = testMapper.getFieldData(
            {"name": "", "code": ""}, testMapping["language"][1]
        )

        assert testData is None
