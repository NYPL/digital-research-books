from PIL import Image, UnidentifiedImageError
import pytest

from managers import CoverManager


class TestCoverManager:
    @pytest.fixture
    def test_manager(self, mocker):
        class MockCoverManager(CoverManager):
            def __init__(self, identifiers, db_session):
                self.identifiers = identifiers
                self.db_session = db_session

        return MockCoverManager([(1, 'test')], mocker.MagicMock())

    @pytest.fixture
    def mock_image_creator(self, mocker):
        def create_image_mocks(width, height, format):
            mock_original = mocker.MagicMock(width=width, height=height, format=format)
            mock_resized = mocker.MagicMock()

            mock_original.resize.return_value = mock_resized

            return mock_original

        return create_image_mocks

    def test_load_fetchers(self, test_manager):
        test_manager.load_fetchers()

        assert len(test_manager.fetchers) == 4
        assert test_manager.fetchers[0].__name__ == 'HathiFetcher'
        assert test_manager.fetchers[3].__name__ == 'ContentCafeFetcher'

    def test_fetch_cover_success(self, test_manager, mocker):
        mock_fetcher = mocker.MagicMock()
        mock_fetcher.has_cover.return_value = True
        test_manager.fetchers = [mocker.MagicMock(return_value=mock_fetcher)]

        assert test_manager.fetch_cover() == True

        test_manager.fetchers[0].assert_called_once_with([(1, 'test')], test_manager.db_session)
        mock_fetcher.has_cover.assert_called_once()

    def test_fetch_cover_none(self, test_manager, mocker):
        mock_fetcher = mocker.MagicMock()
        mock_fetcher.has_cover.return_value = False
        test_manager.fetchers = [mocker.MagicMock(return_value=mock_fetcher)]

        assert test_manager.fetch_cover() == False

    def test_fetch_cover_file(self, test_manager, mocker):
        test_manager.fetcher = mocker.MagicMock()
        test_manager.fetcher.download_cover_file.return_value = 'test_content'

        test_manager.fetch_cover_file()

        assert test_manager.cover_content == 'test_content'
        test_manager.fetcher.download_cover_file.assert_called_once()

    def test_resize_cover_tall(self, test_manager, mock_image_creator, mocker):
        mock_image = mocker.patch.object(Image, 'open')

        mock_original = mock_image_creator(500, 900, 'test')
        mock_image.return_value = mock_original

        test_manager.cover_content = b'test_image_bytes'
        test_manager.resize_cover_file()

        assert test_manager.cover_content == b''
        assert test_manager.cover_format == 'test'
        mock_original.resize.assert_called_once_with((222, 400))

    def test_resize_cover_short(self, test_manager, mock_image_creator, mocker):
        mock_image = mocker.patch.object(Image, 'open')

        mock_original = mock_image_creator(900, 500, 'test')
        mock_image.return_value = mock_original

        test_manager.cover_content = b'test_image_bytes'
        test_manager.resize_cover_file()

        assert test_manager.cover_content == b''
        assert test_manager.cover_format == 'test'
        mock_original.resize.assert_called_once_with((300, 167))

    def test_resize_cover_square(self, test_manager, mock_image_creator, mocker):
        mock_image = mocker.patch.object(Image, 'open')

        mock_original = mock_image_creator(500, 500, 'test')
        mock_image.return_value = mock_original

        test_manager.cover_content = b'test_image_bytes'
        test_manager.resize_cover_file()

        assert test_manager.cover_content == b''
        assert test_manager.cover_format == 'test'
        mock_original.resize.assert_called_once_with((300, 300))

    def test_resize_cover_error(self, test_manager, mocker):
        mock_image = mocker.patch.object(Image, 'open')
        mock_image.side_effect = UnidentifiedImageError

        test_manager.cover_content = b'test_image_bytes'

        assert test_manager.resize_cover_file() == None
        assert test_manager.cover_content == None
        
