import pytest
from model import Record


class TestModelRecord:
    @pytest.fixture
    def testRecord(self):
        return Record(
            uuid="Test UUID",
            title="Test Title",
            languages=["English|en|eng"],
            has_version="first edition",
        )

    # Test for setter method when edition is None or empty list
    def test_no_language(self, testRecord):
        testRecord.languages = None
        testRecord.has_version = "first edition"
        assert testRecord.has_version == "first edition|1"

    # Test to assert that the setter properly handles null values
    def test_no_edition(self, testRecord):
        testRecord.has_version = None
        assert testRecord.has_version is None

    # Failed Test for setter method
    def test_has_version_failure(self, testRecord):
        testRecord.has_version = "other edition"
        assert testRecord.has_version == "other edition|None"

    # Test for setter method when edition is in numeric form
    def test_has_version_numeric(self, testRecord):
        testRecord.has_version = "2nd edition"
        assert testRecord.has_version == "2nd edition|2"

    # Test for setter method when edition is in English
    def test_has_version_english(self, testRecord):
        testRecord.has_version = "first edition"
        assert testRecord.has_version == "first edition|1"

    # Test for setter method when edition is in French
    def test_has_version_french(self, testRecord):
        testRecord.languages = ["French|fr|fre"]
        testRecord.has_version = "deuxième edition"
        assert testRecord.has_version == "deuxième edition|2"

    # Test for setter method when edition is in German
    def test_has_version_german(self, testRecord):
        testRecord.languages = ["German|ge|dt"]
        testRecord.has_version = "zweite edition"
        assert testRecord.has_version == "zweite edition|2"

    # Test for setter method when edition is in Spanish
    def test_has_version_spanish(self, testRecord):
        testRecord.languages = ["Spanish|spa|span"]
        testRecord.has_version = "segundo edition"
        assert testRecord.has_version == "segundo edition|2"

    # Test for setter method when edition is in Dutch
    def test_has_version_dutch(self, testRecord):
        testRecord.languages = ["Dutch|du|dut"]
        testRecord.has_version = "tweede edition"
        assert testRecord.has_version == "tweede edition|2"

    # Test for setter method when edition is in Russian
    def test_has_version_russian(self, testRecord):
        testRecord.languages = ["Russian|ru|rus"]
        testRecord.has_version = "второй edition"
        assert testRecord.has_version == "второй edition|2"
