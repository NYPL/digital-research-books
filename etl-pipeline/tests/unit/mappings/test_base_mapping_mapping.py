import pytest

from mappings.base_mapping import BaseMapping


class TestCoreMapping:
    @pytest.fixture
    def testMapping(self, mocker):
        class TestCore(BaseMapping):
            def ___init__(self, source, constants):
                super(self, TestCore).__init__(source, constants)

            def createMapping(self):
                pass

        mockFormatter = mocker.patch("mappings.base_mapping.CustomFormatter")
        mockFormatter.return_value = "mockFormatter"
        return TestCore("test", {"static": "values"})

    def test_initializer(self, testMapping):
        assert testMapping.mapping == {}
        assert testMapping.source == "test"
        assert testMapping.record == None
        assert testMapping.constants == {"static": "values"}
        assert testMapping.formatter == "mockFormatter"

    def test_initEmptyRecord(self, testMapping, mocker):
        mockUUID = mocker.patch("mappings.base_mapping.uuid4")
        mockUUID.return_value = "testUUID"
        mockDate = mocker.patch("mappings.base_mapping.datetime")
        mockDate.now.return_value.replace.side_effect = ["testCreated", "testModified"]
        mockRecord = mocker.patch("mappings.base_mapping.Record")
        mockRecord.return_value = "testRecord"

        testRecord = testMapping.initEmptyRecord()

        assert testRecord == "testRecord"
        mockRecord.assert_called_once_with(
            uuid="testUUID",
            date_created="testCreated",
            date_modified="testModified",
            frbr_status="to_do",
            cluster_status=False,
        )

    def test_applyMapping(self, testMapping, mocker):
        mockInitEmpty = mocker.patch.object(BaseMapping, "initEmptyRecord")

        testMapping.applyMapping()

        mockInitEmpty.assert_called_once

    def test_applyFormatting(self, testMapping):
        assert testMapping.applyFormatting() is None

    def test_updateExisting(self, testMapping, mocker):
        mockRecord = mocker.MagicMock()
        mockRecord.__iter__.return_value = [("uuid", "uuid1"), ("title", "Test Title")]
        testMapping.record = mockRecord

        mockExisting = mocker.MagicMock(
            uuid="uuid2", source="test", frbr_status="complete"
        )

        testMapping.updateExisting(mockExisting)

        assert mockExisting.uuid == "uuid2"
        assert mockExisting.title == "Test Title"
        assert mockExisting.frbr_status == "to_do"
        assert mockExisting.cluster_status == False

    def test_updateExisting_oclc(self, testMapping, mocker):
        mockRecord = mocker.MagicMock()
        mockRecord.__iter__.return_value = [("uuid", "uuid1"), ("title", "Test Title")]
        testMapping.record = mockRecord

        mockExisting = mocker.MagicMock(
            uuid="uuid2", source="oclcClassify", frbr_status="complete"
        )

        testMapping.updateExisting(mockExisting)

        assert mockExisting.uuid == "uuid2"
        assert mockExisting.title == "Test Title"
        assert mockExisting.frbr_status == "complete"
        assert mockExisting.cluster_status == False
