from lxml import etree
import pytest

from mappings.xml import XMLMapping


class TestXMLMapping:
    @pytest.fixture
    def testMapper(self):
        class TestXML(XMLMapping):
            def __init__(self):
                super().__init__(None, None, None)

            def createMapping(self):
                pass

        return TestXML()

    @pytest.fixture
    def testMapping(self):
        return {
            "title": ("//data/title/text()", "{0}"),
            "identifiers": [
                ("//data/isbn/text()", "{0}|isbn"),
                ("//data/oclc/text()", "{0}|oclc"),
            ],
            "dates": (["//data/date/text()", "//data/date/@type"], "{0}|{1}"),
        }

    @pytest.fixture
    def testXML(self):
        xmlString = """<data>
            <title>Test Title</title>
            <isbn>1234</isbn>
            <oclc>9876</oclc>
            <date type="published">1900</date>
        </data>
        """

        return etree.fromstring(xmlString)

    def test_applyMapping(self, testMapper, testMapping, mocker):
        mapperMocks = mocker.patch.multiple(
            XMLMapping,
            initEmptyRecord=mocker.DEFAULT,
            getFieldData=mocker.DEFAULT,
            applyFormatting=mocker.DEFAULT,
        )
        mapperMocks["initEmptyRecord"].return_value = mocker.MagicMock()
        mapperMocks["getFieldData"].side_effect = [
            ["Test Title"],
            ["1234|isbn"],
            ["|oclc"],
            IndexError,
        ]

        testMapper.mapping = testMapping

        testMapper.applyMapping()

        assert testMapper.record.title == "Test Title"
        assert testMapper.record.identifiers == ["1234|isbn"]
        mapperMocks["applyFormatting"].assert_called_once

    def test_getFieldData_single(self, testMapper, testMapping, testXML):
        testMapper.source = testXML

        testData = testMapper.getFieldData(testMapping["title"])

        assert testData == ["Test Title"]

    def test_getFieldData_combined(self, testMapper, testMapping, testXML):
        testMapper.source = testXML

        testData = testMapper.getFieldData(testMapping["dates"])

        assert testData == ["1900|published"]

    def test_filterEmptyFields_value_present(self, testMapper):
        assert testMapper.filterEmptyFields(["", "hello", "world"]) is True

    def test_filterEmptyFields_value_missing(self, testMapper):
        assert testMapper.filterEmptyFields(["", "", ""]) is False
