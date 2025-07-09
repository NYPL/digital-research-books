from flask import Flask
import pytest

from api.blueprints.drbFulfill import fulfill_item, fulfill
from api.utils import APIUtils
from model.postgres.link import Link


class TestSearchBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            formatResponseObject=mocker.DEFAULT,
            getPresignedUrlFromObjectUrl="example.com/example.pdf",
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"
        flask_app.config["READER_VERSION"] = "test"
        flask_app.register_blueprint(fulfill)

        return flask_app

    @pytest.fixture
    def mock_db(self, mocker, monkeypatch):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db.fetchSingleLink.return_value = Link(
            flags={"nypl_login": True},
            url="https://doc-example-bucket1.s3.us-west-2.amazonaws.com/puppy.png",
        )
        mocker.patch("api.blueprints.drbFulfill.DBClient").return_value = mock_db

        monkeypatch.setenv("NYPL_API_CLIENT_PUBLIC_KEY", "SomeKeyValue")

    @pytest.fixture
    def mock_s3(self, mocker):
        mock_s3_manager = mocker.MagicMock()
        mocker.patch(
            "api.blueprints.drbFulfill.S3Manager"
        ).return_value = mock_s3_manager
        return mock_s3_manager

    def test_fulfill_item_invalid_token(self, test_app, mock_utils, mock_db):
        with test_app.test_request_context(
            "/fulfill/12345", headers={"Authorization": "Bearer Whatever"}
        ):
            fulfill_item("12345")
            mock_utils["formatResponseObject"].assert_called_once_with(
                401,
                "fulfill",
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def test_fulfill_item_no_bearer_auth(self, test_app, mock_utils, mock_db):
        with test_app.test_request_context(
            "/fulfill/12345", headers={"Authorization": "Whatever"}
        ):
            fulfill_item("12345")
            mock_utils["formatResponseObject"].assert_called_once_with(
                401,
                "fulfill",
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def test_fulfill_item_empty_token(self, test_app, mock_utils, mock_db):
        with test_app.test_request_context(
            "/fulfill/12345", headers={"Authorization": ""}
        ):
            fulfill_item("12345")
            mock_utils["formatResponseObject"].assert_called_once_with(
                401,
                "fulfill",
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def test_fulfill_item_no_header(self, test_app, mock_utils, mock_db):
        with test_app.test_request_context("/fulfill/12345"):
            fulfill_item("12345")
            mock_utils["formatResponseObject"].assert_called_once_with(
                401,
                "fulfill",
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def test_fulfill_item_invalid_iss(self, test_app, mock_utils, mock_db, mocker):
        with test_app.test_request_context("/fulfill/12345"):
            mocker.patch("jwt.decode", return_value={"iss": "https://example.com"})
            fulfill_item("12345")
            mock_utils["formatResponseObject"].assert_called_once_with(
                401,
                "fulfill",
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def test_fulfill_item_redirect(self, test_app, mock_s3, mock_db, mocker):
        mocker.patch(
            "api.utils.APIUtils.getPresignedUrlFromObjectUrl",
            return_value="example.com/example.pdf",
        )
        mocker.patch("jwt.decode", return_value={"iss": "https://www.nypl.org"})
        response = test_app.test_client().get(
            "/fulfill/12345",
            follow_redirects=False,
            headers={"Authorization": "Bearer Whatever"},
        )
        assert response.status_code == 302
        assert response.location == "example.com/example.pdf"

    def test_fulfill_item_invalid_link_id(self, test_app, mock_utils):
        with test_app.test_request_context("/fulfill/invalid-link-id"):
            fulfill_item("invalid-link-id")
            mock_utils["formatResponseObject"].assert_called_once_with(
                400,
                "fulfill",
                {"message": "Link id invalid-link-id is invalid"},
            )
