from bs4 import BeautifulSoup
import pytest
import requests
from requests.exceptions import ReadTimeout

from managers import MUSEManager, MUSEError
from tests.helper import TestHelpers


class TestMUSEManager:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def test_manager(self, mocker):
        class TestMUSE(MUSEManager):
            def __init__(self):
                self.record = mocker.MagicMock(record="test_record")
                self.muse_id = 1
                self.link = "test_link"
                self.media_type = "test_type"
                self.s3_bucket = "test_aws_bucket"

                self.pdf_download_url = None
                self.epub_url = None
                self.pdf_webpub_manifest = None

        return TestMUSE()

    @pytest.fixture
    def test_muse_page(self):
        return open("./tests/fixtures/muse_book_42.html", "r").read()

    @pytest.fixture
    def test_muse_page_unreleased(self):
        return open("./tests/fixtures/muse_book_63320.html", "r").read()

    def test_parse_muse_page_success(self, test_manager, mocker):
        mock_load = mocker.patch.object(MUSEManager, "load_muse_page")
        mock_load.return_value = "test_html"
        mock_soup = mocker.patch("managers.muse.BeautifulSoup")
        mock_soup.return_value = "test_soup"

        test_manager.parse_muse_page()

        assert test_manager.muse_soup == "test_soup"
        mock_load.assert_called_once()
        mock_soup.assert_called_once_with("test_html", features="lxml")

    def test_parse_muse_page_error(self, test_manager, mocker):
        mock_load = mocker.patch.object(MUSEManager, "load_muse_page")
        mock_load.side_effect = ReadTimeout
        mock_soup = mocker.patch("managers.muse.BeautifulSoup")

        with pytest.raises(MUSEError):
            test_manager.parse_muse_page()

        mock_soup.assert_not_called()

    def test_identify_readable_versions_w_pdf(self, test_manager, mocker):
        base_soup = mocker.MagicMock()
        chapter_soup = mocker.MagicMock()
        button_soup = mocker.MagicMock()
        button_soup.parent = {"href": "/pdfURL"}

        base_soup.find.return_value = chapter_soup
        chapter_soup.find.side_effect = [button_soup, None]

        test_manager.muse_soup = base_soup

        test_manager.identify_readable_versions()

        assert test_manager.pdf_download_url == "https://muse.jhu.edu/pdfURL"
        base_soup.find.assert_called_once_with(id="available_items_list_wrap")
        chapter_soup.find.assert_has_calls(
            [mocker.call(alt="Download PDF"), mocker.call(alt="Download EPUB")]
        )

    def test_identify_readable_versions_w_epub(self, test_manager, mocker):
        base_soup = mocker.MagicMock()
        chapter_soup = mocker.MagicMock()
        button_soup = mocker.MagicMock()
        button_soup.parent = {"href": "/epubURL"}

        base_soup.find.return_value = chapter_soup
        chapter_soup.find.side_effect = [None, button_soup]

        test_manager.muse_soup = base_soup

        test_manager.identify_readable_versions()

        assert test_manager.epub_url == "https://muse.jhu.edu/epubURL"
        base_soup.find.assert_called_once_with(id="available_items_list_wrap")
        chapter_soup.find.assert_has_calls(
            [mocker.call(alt="Download PDF"), mocker.call(alt="Download EPUB")]
        )

    def test_identify_readable_versions_none(self, test_manager, mocker):
        base_soup = mocker.MagicMock()

        base_soup.find.return_value = None

        test_manager.muse_soup = base_soup

        assert test_manager.identify_readable_versions() is None

    def test_add_readable_links_pdf(self, test_manager, mocker):
        mock_construct = mocker.patch.object(MUSEManager, "construct_webpub_manifest")

        test_manager.pdf_download_url = "testPDFURL"

        test_manager.add_readable_links()

        mock_construct.assert_called_once()

    def test_add_readable_links_epub(self, test_manager, mocker):
        mock_construct = mocker.patch.object(MUSEManager, "construct_webpub_manifest")
        mock_s3 = mocker.patch.object(MUSEManager, "construct_s3_link")
        mock_s3.side_effect = ["epub_download_url", "epub_read_url"]

        test_manager.epub_url = "test_epub_url"

        test_manager.add_readable_links()

        mock_s3.assert_has_calls(
            [
                mocker.call("epubs/muse/1.epub"),
                mocker.call("epubs/muse/1/manifest.json"),
            ]
        )
        mock_construct.assert_called_once()

    def test_add_readable_links_manifest(self, test_manager, mocker):
        mock_construct = mocker.patch.object(MUSEManager, "construct_webpub_manifest")
        mock_s3 = mocker.patch.object(MUSEManager, "create_manifest_in_s3")
        mock_s3.return_value = "webpub_read_url"

        test_manager.pdf_webpub_manifest = "test_manifest_url"

        test_manager.add_readable_links()

        mock_s3.assert_called_once_with("manifests/muse/1.json")
        mock_construct.assert_called_once()

    def test_construct_webpub_manifest(self, test_manager, test_muse_page, mocker):
        test_manager.muse_soup = BeautifulSoup(test_muse_page, features="lxml")

        mock_manifest = mocker.MagicMock(readingOrder=[1, 2, 3])
        mock_manifest_constructor = mocker.patch("managers.muse.WebpubManifest")
        mock_manifest_constructor.return_value = mock_manifest

        test_manager.construct_webpub_manifest()

        assert test_manager.pdf_webpub_manifest == mock_manifest

        mock_manifest_constructor.assert_called_once_with("test_link", "test_type")
        mock_manifest.addMetadata.assert_called_once_with(test_manager.record)

        mock_manifest.addSection.assert_has_calls(
            [
                mocker.call("Part One. Reading Reading Historically", ""),
                mocker.call(
                    "PART TWO. Contextual Receptions, Reading Experiences, and Patterns of Response: Four Case Studies",
                    "",
                ),
            ]
        )

        mock_manifest.addChapter.assert_has_calls(
            [
                mocker.call("https://muse.jhu.edu/chapter/440/pdf", "Cover"),
                mocker.call(
                    "https://muse.jhu.edu/chapter/2183675/pdf?start=2", "Title Page"
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/2183674/pdf?start=2", "Copyright"
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/2183673/pdf?start=2", "Dedication"
                ),
                mocker.call("https://muse.jhu.edu/chapter/441/pdf?start=2", "Contents"),
                mocker.call("https://muse.jhu.edu/chapter/442/pdf?start=2", "Preface"),
                mocker.call(
                    "https://muse.jhu.edu/chapter/444/pdf?start=2",
                    "Chapter 1. Historical Hermeneutics, Reception Theory, and the Social Conditions of Reading in Antebellum America",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/445/pdf?start=2",
                    "Chapter 2. Interpretive Strategies and Informed Reading in the Antebellum Public Sphere",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/6239/pdf?start=2",
                    "Chapter 3. “These Days of Double Dealing”: Informed Response, Reader Appropriation, and the Tales of Poe",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/6240/pdf?start=2",
                    "Chapter 4. Multiple Audiences and Melville’s Fiction: Receptions, Recoveries, and Regressions",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/6241/pdf?start=2",
                    "Chapter 5. Response as (Re)construction: The Reception of Catharine Sedgwick’s Novels",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/6242/pdf?start=2",
                    "Chapter 6. Mercurial Readings: The Making and Unmaking of Caroline Chesebro’",
                ),
                mocker.call(
                    "https://muse.jhu.edu/chapter/6243/pdf?start=2",
                    "Conclusion. American Literary History and the Historical Study of Interpretive Practices",
                ),
                mocker.call("https://muse.jhu.edu/chapter/6244/pdf?start=2", "Notes"),
                mocker.call("https://muse.jhu.edu/chapter/6245/pdf?start=2", "Index"),
            ]
        )

        mock_manifest.closeSection.assert_has_calls([mocker.call(), mocker.call()])

    def test_load_muse_page_success(self, test_manager, mocker):
        mock_get = mocker.patch.object(requests, "get")
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "test_html"
        mock_get.return_value = mock_resp

        assert test_manager.load_muse_page() == "test_html"
        mock_get.assert_called_once_with(
            "test_link", timeout=15, headers={"User-agent": "Mozilla/5.0"}
        )

    def test_load_muse_page_error(self, test_manager, mocker):
        mock_get = mocker.patch.object(requests, "get")
        mock_resp = mocker.MagicMock()
        mock_resp.raise_for_status.side_effect = Exception
        mock_get.return_value = mock_resp

        with pytest.raises(Exception):
            test_manager.load_muse_page()

    def test_construct_s3_link(self, test_manager):
        assert (
            test_manager.construct_s3_link("test_location")
            == "https://test_aws_bucket.s3.amazonaws.com/test_location"
        )

    def test_create_manifest_in_s3(self, test_manager, mocker):
        mock_construct = mocker.patch.object(MUSEManager, "construct_s3_link")
        mock_construct.return_value = "test_s3_url"

        test_manager.pdf_webpub_manifest = mocker.MagicMock()
        test_manager.pdf_webpub_manifest.links = []

        test_url = test_manager.create_manifest_in_s3("test_path")

        assert test_url == "test_s3_url"
        assert test_manager.pdf_webpub_manifest.links[0] == {
            "href": test_url,
            "type": "application/webpub+json",
            "rel": "self",
        }
        mock_construct.assert_called_once_with("test_path")
