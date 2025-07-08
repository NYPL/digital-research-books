import pytest

from mappings.marc import MARCMapping


class TestMARCMapping:
    @pytest.fixture
    def testFields(self):
        return {
            "title": ("245", "{a}"),
            "identifiers": [("020", "{a}|isbn"), ("022", "{a}|issn")],
            "is_part_of": ("490", "{a}||volume"),
        }

    @pytest.fixture
    def testRecord(self, mocker):
        testRecord = mocker.MagicMock()
        testRecord.title = ["Test Title", "Secondary Title"]
        testRecord.identifiers = ["1|muse", "|test", "3|other"]

        return testRecord

    @pytest.fixture
    def testMapper(self, testFields, testRecord):
        class TestMARC(MARCMapping):
            def __init__(self, source, mapping, constants):
                super().__init__(source, constants)
                self.source = source
                self.mapping = mapping
                self.constants = constants

            def createMapping(self):
                pass

            def initEmptyRecord(self):
                return testRecord

        return TestMARC({}, testFields, {})

    def test_applyMapping(self, testMapper, mocker):
        mappingMocks = mocker.patch.multiple(
            MARCMapping, getFieldData=mocker.DEFAULT, applyFormatting=mocker.DEFAULT
        )

        mappingMocks["getFieldData"].side_effect = [
            ["Test Title"],
            ["1|isbn"],
            ["|issn"],
            IndexError,
        ]

        testMapper.applyMapping()

        mappingMocks["applyFormatting"].assert_called_once
        assert testMapper.record.title == "Test Title"
        assert testMapper.record.identifiers == ["1|isbn"]

    def test_getFieldData(self, testMapper, mocker):
        mockGetMARC = mocker.patch.object(MARCMapping, "getMARCData")
        mockGetMARC.return_value = "testMARCData"

        mockSetField = mocker.patch.object(MARCMapping, "setFieldValues")
        mockSetField.return_value = ["value1", "value2"]

        testFieldData = testMapper.getFieldData(("000", "testStruct"))

        assert testFieldData == ["value1", "value2"]
        mockGetMARC.assert_called_once_with("000")
        mockSetField.assert_called_once_with("testMARCData", "testStruct")

    def test_getMARCData_controlField(self, testMapper, mocker):
        mockControlField = mocker.MagicMock(data="testValue")
        mockControlField.is_control_field.return_value = True

        testMapper.source = mocker.MagicMock()
        testMapper.source.get_fields.return_value = [mockControlField]

        testData = testMapper.getMARCData("000")

        assert testData == ["testValue"]
        mockControlField.is_control_field.assert_called_once

    def test_getMARCData_standardField(self, testMapper, mocker):
        mockField = mocker.MagicMock(
            subfields=["a", "aValue", "b", "bValue", "c", "cValue"],
            indicators=["ind1Value", "ind2Value"],
        )
        mockField.is_control_field.return_value = False

        testMapper.source = mocker.MagicMock()
        testMapper.source.get_fields.return_value = [mockField]

        testData = testMapper.getMARCData("000")

        assert testData == [
            {
                "ind1": "ind1Value",
                "ind2": "ind2Value",
                "a": "aValue",
                "b": "bValue",
                "c": "cValue",
            }
        ]
        mockField.is_control_field.assert_called_once

    def test_setFieldValues(self, testMapper, mocker):
        mockApply = mocker.patch.object(MARCMapping, "applyStringFormat")
        mockApply.side_effect = ["fieldValue", KeyError]

        testValues = [
            v
            for v in testMapper.setFieldValues(
                [None, "marcValue", "marcValue2"], "testFormat"
            )
        ]

        assert testValues == ["fieldValue"]
        mockApply.assert_has_calls(
            [
                mocker.call("marcValue", "testFormat"),
                mocker.call("marcValue2", "testFormat"),
            ]
        )

    def test_applyStringFormat_string(self, testMapper, mocker):
        assert (
            testMapper.applyStringFormat("Test String    ", "{0}||true")
            == "Test String||true"
        )

    def test_applyStringFormat_dict(self, testMapper, mocker):
        assert (
            testMapper.applyStringFormat(
                {"a": "Last,", "b": "First", "c": "M.I."}, "{a} {b} {c}||true"
            )
            == "Last, First M.I.||true"
        )
