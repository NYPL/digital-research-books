import os
import pytest
from services import GoogleDriveService


class TestGoogleDriveService:
    @pytest.fixture
    def test_instance(self):
        return GoogleDriveService()

    def test_get_drive_file(self, test_instance: GoogleDriveService):
        TEST_FILE_ID = "1GBskazIv6j2BjOolNYqJKKIJC8iYhTcs"
        file = test_instance.get_drive_file(TEST_FILE_ID)

        assert file != None
        assert file.seek(0, 2) > 1
