import pytest

from api.opds2 import Image


class TestImage:
    @pytest.fixture
    def testImage(self):
        class TestImage(Image):
            def __init__(self):
                pass

        return TestImage()

    def test_repr(self, testImage):
        testImage.href = "testHref"
        testImage.type = "testType"

        assert str(testImage) == '<Image(href="testHref", type="testType")>'
