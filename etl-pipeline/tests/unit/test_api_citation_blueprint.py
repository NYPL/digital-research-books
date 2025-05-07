from flask import Flask
import pytest
from datetime import date

from api.blueprints.drbCitation import get_citation
from api.utils import APIUtils


class TestCitationBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            formatResponseObject=mocker.DEFAULT,
            normalizeQueryParams=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"

        return flask_app

    def test_get_citation_bible_pre1900_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1), publication_place="testPubPlace"
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="The Bible", editions=edition_mocks
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {"mla": "The Bible. Unknown version, testPubPlace, 1800."},
            )

    def test_get_citation_bible_post1900_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1), publishers=[{"name": "NYPL"}]
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="The Bible", editions=edition_mocks
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "citation", {"mla": "The Bible. Unknown version, NYPL, 2022."}
            )

    def test_get_citation_post1900_gov_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                measurements=[{"type": "government_document", "value": "1"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="State of the Union Address",
            authors=[{"name": "United States. Test Agency"}],
            editions=edition_mocks,
        )
        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "United States, Test Agency. State of the Union Address. \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_trans_editor_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            contributors=[
                {"roles": "translator", "name": "transLastName, transFirstName"},
                {"roles": "editor", "name": "editLastName, editFirstName"},
            ],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testTitle. Translated by transFirstName \
                                transLastName, edited by editFirstName editLastName, \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_trans_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            contributors=[
                {"roles": "translator", "name": "transLastName, transFirstName"}
            ],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testTitle. Translated by transFirstName \
                                transLastName, \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_single_edit_work_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                edition_statement=None,
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle", editions=edition_mocks
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testTitle. \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_mult_edit_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                edition_statement="2nd edition.",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle", editions=edition_mocks
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testTitle. 2nd edition., \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_one_author_work(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[{"name": "testLastName, testFirstName"}],
            contributors=[{"roles": "editor", "name": "editLastName, editFirstName"}],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName. testTitle, \
                                edited by editFirstName editLastName, \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_one_author_work2(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                edition_statement=None,
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[{"name": "testLastName, testFirstName"}],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName. testTitle. \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_corporate_work(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                edition_statement=None,
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle", authors=[{"name": "NYPL"}], editions=edition_mocks
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testTitle. \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_two_authors_work(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[
                {"name": "testLastName, testFirstName"},
                {"name": "testLastName2, testFirstName2"},
            ],
            contributors=[{"roles": "editor", "name": "editLastName, editFirstName"}],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName, and testFirstName2 testLastName2. \
                                testTitle, edited by editFirstName editLastName, \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_two_authors_work2(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                edition_statement="2nd edition.",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[
                {"name": "testLastName, testFirstName"},
                {"name": "testLastName2, testFirstName2"},
            ],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName, and testFirstName2 testLastName2. \
                                testTitle. 2nd edition., \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_three_authors_work(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(1800, 1, 1),
                publication_place="testPubPlace",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[
                {"name": "testLastName, testFirstName"},
                {"name": "testLastName2, testFirstName2"},
                {"name": "testLastName3, testFirstName3"},
            ],
            contributors=[{"roles": "editor", "name": "editLastName, editFirstName"}],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName, et al. \
                                testTitle, edited by editFirstName editLastName, \
                                testPubPlace, 1800.".split()
                    )
                },
            )

    def test_get_citation_three_authors_work2(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        edition_mocks = [
            mocker.MagicMock(
                publication_date=date(2022, 1, 1),
                publishers=[{"name": "NYPL"}],
                edition_statement="2nd edition.",
                measurements=[{"type": "government_document", "value": "0"}],
            )
        ]

        mock_db.fetchSingleWork.return_value = mocker.MagicMock(
            title="testTitle",
            authors=[
                {"name": "testLastName, testFirstName"},
                {"name": "testLastName2, testFirstName2"},
                {"name": "testLastName3, testFirstName3"},
            ],
            editions=edition_mocks,
        )

        mock_utils["formatResponseObject"].return_value = "citationResponse"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "citationResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "citation",
                {
                    "mla": " ".join(
                        "testLastName, testFirstName, et al. \
                                testTitle. 2nd edition., \
                                NYPL, 2022.".split()
                    )
                },
            )

    def test_get_citation_fail(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        mock_db.fetchSingleWork.return_value = None

        mock_utils["formatResponseObject"].return_value = "404Response"

        with test_app.test_request_context(
            "a8512b02-779b-45c6-95a3-56f90831be46/?format=mla"
        ):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "404Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                404,
                "citation",
                {
                    "message": "No work found with id a8512b02-779b-45c6-95a3-56f90831be46"
                },
            )

    def test_get_citation_empty_format(self, mock_utils, test_app):
        mock_utils["formatResponseObject"].return_value = "400Response"

        with test_app.test_request_context("/?format="):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "400Response"

            mock_utils["formatResponseObject"].assert_called_once_with(
                400, "citation", {"message": "Citation formats are invalid"}
            )

    def test_get_citation_missing_format(self, mock_utils, test_app):
        mock_utils["formatResponseObject"].return_value = "400response"

        with test_app.test_request_context("/"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "400response"

            mock_utils["formatResponseObject"].assert_called_once_with(
                400, "citation", {"message": "Citation formats are invalid"}
            )

    def test_get_citation_unknown_format(self, mock_utils, test_app):
        mock_utils["formatResponseObject"].return_value = "400response"

        with test_app.test_request_context("/?format=test"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "400response"

            mock_utils["formatResponseObject"].assert_called_once_with(
                400, "citation", {"message": "Citation formats are invalid"}
            )

    def test_get_citation_error(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbCitation.DBClient", return_value=mock_db
        )

        mock_db.fetchSingleEdition.side_effect = Exception("Database error")

        mock_utils["formatResponseObject"].return_value = "500response"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "500response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                500,
                "citation",
                {
                    "message": "Unable to get citation for work with id a8512b02-779b-45c6-95a3-56f90831be46"
                },
            )

    def test_get_citation_invalid_id(self, mock_utils, test_app):
        mock_utils["formatResponseObject"].return_value = "400response"

        with test_app.test_request_context("/?format=mla"):
            test_api_response = get_citation("testUUID")

            assert test_api_response == "400response"

            mock_utils["formatResponseObject"].assert_called_once_with(
                400, "citation", {"message": "Work id testUUID is invalid"}
            )
