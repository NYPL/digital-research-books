import pytest

from managers.kMeans import YearObject


class TestYearObject:
    @pytest.fixture
    def testObject(self):
        return YearObject(1900, 2000)

    def test_initializer(self, testObject):
        assert testObject.start == "1900"
        assert testObject.end == "2000"
        assert testObject.century == [None, None]
        assert testObject.decade == [None, None]
        assert testObject.year == [None, None]

    def test_setYearComponents(self, testObject, mocker):
        objectMocks = mocker.patch.multiple(
            YearObject,
            setCentury=mocker.DEFAULT,
            setDecade=mocker.DEFAULT,
            setYear=mocker.DEFAULT,
        )

        testObject.setYearComponents()

        objectMocks["setCentury"].assert_called_once()
        objectMocks["setDecade"].assert_called_once()
        objectMocks["setYear"].assert_called_once()

    def test_setCentury_start_end(self, testObject):
        testObject.setCentury()

        assert testObject.century == [19, 20]

    def test_setCentury_start_only(self, testObject):
        testObject.end = ""
        testObject.setCentury()

        assert testObject.century == [19, None]

    def test_setDecade_start_end(self, testObject):
        testObject.setDecade()

        assert testObject.decade == [0, 0]

    def test_setDecade_start_only(self, testObject):
        testObject.end = ""
        testObject.setDecade()

        assert testObject.decade == [0, None]

    def test_setYear_start_end(self, testObject):
        testObject.setYear()

        assert testObject.year == [0, 0]

    def test_setYear_start_only(self, testObject):
        testObject.end = ""
        testObject.setYear()

        assert testObject.year == [0, None]

    def test_convertYearDictToStr_start_end(self, mocker):
        mockGetYear = mocker.patch.object(YearObject, "getYearStr")
        mockGetYear.side_effect = [1900, 2000]

        assert YearObject.convertYearDictToStr("mockDict") == "1900-2000"

    def test_convertYearDictToStr_start_only(self, mocker):
        mockGetYear = mocker.patch.object(YearObject, "getYearStr")
        mockGetYear.side_effect = [1900, None]

        assert YearObject.convertYearDictToStr("mockDict") == "1900"

    def test_getYearStr(self):
        testYearDict = {"centuryTest": 19, "decadeTest": 9, "yearTest": None}

        assert YearObject.getYearStr(testYearDict, "Test") == "199x"

    def test_iter_for_dict(self, testObject):
        testObject.century = [19, 20]
        testObject.decade = [1, 1]
        testObject.year = [1, None]

        assert dict(testObject) == {
            "centuryStart": 19,
            "centuryEnd": 20,
            "decadeStart": 1,
            "decadeEnd": 1,
            "yearStart": 1,
        }
