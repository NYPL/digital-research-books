import os

import pytest
from flask import Flask

from api.assistant.agent import TOOL_ERROR_PREFIX
from api.blueprints.result_reason import (
    FALLBACK_RESULT_REASON,
    result_reason_blueprint,
)

SEARCH_RESULTS_XML = """\
<search_results>
<edition>
<edition_id>1</edition_id>
<barcode>00000000000001</barcode>
<title>A Tale of Two Cities</title>
</edition>
<edition>
<edition_id>2</edition_id>
<barcode>00000000000002</barcode>
<title>Other Book</title>
</edition>
</search_results>"""


def make_session_messages(
    call_id="call_1",
    tool_output=SEARCH_RESULTS_XML,
    include_function_call=True,
    include_function_call_output=True,
):
    messages = [{"type": "message", "role": "user", "content": "Find something."}]
    if include_function_call:
        messages.append(
            {
                "type": "function_call",
                "call_id": call_id,
                "name": "search_catalog",
                "arguments": '{"ranking_query": "something"}',
            }
        )
    if include_function_call_output:
        messages.append(
            {
                "type": "function_call_output",
                "call_id": call_id,
                "output": tool_output,
            }
        )
    return messages


class TestResultReasonView:
    @pytest.fixture
    def test_app(self):
        app = Flask("test")
        app.config["TESTING"] = True
        app.register_blueprint(result_reason_blueprint)
        return app

    @pytest.fixture
    def client(self, test_app):
        return test_app.test_client()

    @pytest.fixture(autouse=True)
    def bypass_auth(self, mocker):
        mocker.patch("newrelic.agent.add_custom_attribute")
        mocker.patch.dict(
            os.environ,
            {"VRA_API_KEY": "test-key"},  # pragma: allowlist secret
        )
        mocker.patch("api.decorators.verify_session", return_value="test-session")

    def post_result_reason(self, client, call_id="call_1", barcode="00000000000001"):
        client.set_cookie("vra_session", "test-token")
        payload = {}
        if call_id is not None:
            payload["call_id"] = call_id
        if barcode is not None:
            payload["barcode"] = barcode
        return client.post(
            "/result-reason",
            json=payload,
            headers={"X-API-Key": "test-key"},
        )

    # --- 400s ---

    def test_missing_call_id_returns_400(self, client):
        response = self.post_result_reason(client, call_id=None)

        assert response.status_code == 400
        assert response.get_json()["data"]["message"] == "call_id is required"

    def test_missing_barcode_returns_400(self, client):
        response = self.post_result_reason(client, barcode=None)

        assert response.status_code == 400
        assert response.get_json()["data"]["message"] == "barcode is required"

    # --- 404s ---

    def test_no_session_messages_returns_404(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages", return_value=[]
        )

        response = self.post_result_reason(client)

        assert response.status_code == 404
        assert "not found" in response.get_json()["data"]["message"]

    def test_call_id_not_in_session_returns_404(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(call_id="a-different-call-id"),
        )

        response = self.post_result_reason(client, call_id="call_1")

        assert response.status_code == 404
        assert (
            "call_id 'call_1' not found in session"
            in (response.get_json()["data"]["message"])
        )

    def test_missing_function_call_output_returns_404(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(include_function_call_output=False),
        )

        response = self.post_result_reason(client)

        assert response.status_code == 404
        assert (
            "call_id 'call_1' not found in session"
            in (response.get_json()["data"]["message"])
        )

    def test_tool_error_output_returns_404(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(
                tool_output=f"{TOOL_ERROR_PREFIX}. Please try again. Error: boom"
            ),
        )

        response = self.post_result_reason(client)

        assert response.status_code == 404
        assert "is an error" in response.get_json()["data"]["message"]

    def test_barcode_not_in_tool_output_returns_404(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(),
        )

        response = self.post_result_reason(client, barcode="99999999999999")

        assert response.status_code == 404
        assert (
            "barcode '99999999999999' not found"
            in (response.get_json()["data"]["message"])
        )

    # --- LLM fallback paths (200, is_ai_generated=False) ---

    def test_llm_returns_none_uses_fallback(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(),
        )
        mocker.patch.dict(
            os.environ,
            {"GOOGLE_API_KEY": "fake-key"},  # pragma: allowlist secret
        )
        mock_client = mocker.MagicMock()
        mock_client.chat.completions.create.return_value.choices = [
            mocker.MagicMock(message=mocker.MagicMock(content=None))
        ]
        mocker.patch("api.blueprints.result_reason.OpenAI", return_value=mock_client)

        response = self.post_result_reason(client)

        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["explanation"] == FALLBACK_RESULT_REASON
        assert data["is_ai_generated"] is False

    def test_llm_raises_uses_fallback(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(),
        )
        mocker.patch.dict(
            os.environ,
            {"GOOGLE_API_KEY": "fake-key"},  # pragma: allowlist secret
        )
        mock_client = mocker.MagicMock()
        mock_client.chat.completions.create.side_effect = RuntimeError(
            "LLM unavailable"
        )
        mocker.patch("api.blueprints.result_reason.OpenAI", return_value=mock_client)

        response = self.post_result_reason(client)

        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["explanation"] == FALLBACK_RESULT_REASON
        assert data["is_ai_generated"] is False

    def test_llm_success_is_ai_generated_true(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            return_value=make_session_messages(),
        )
        mocker.patch.dict(
            os.environ,
            {"GOOGLE_API_KEY": "fake-key"},  # pragma: allowlist secret
        )
        mock_client = mocker.MagicMock()
        mock_client.chat.completions.create.return_value.choices = [
            mocker.MagicMock(
                message=mocker.MagicMock(content="A generated explanation.")
            )
        ]
        mocker.patch("api.blueprints.result_reason.OpenAI", return_value=mock_client)

        response = self.post_result_reason(client)

        assert response.status_code == 200
        data = response.get_json()["data"]
        assert data["explanation"] == "A generated explanation."
        assert data["is_ai_generated"] is True

    # --- 500 ---

    def test_unexpected_error_returns_500_and_logs(self, client, mocker):
        mocker.patch(
            "api.blueprints.result_reason.get_session_messages",
            side_effect=RuntimeError("something went wrong"),
        )
        mock_logger = mocker.patch("api.blueprints.result_reason.logger")

        response = self.post_result_reason(client)

        assert response.status_code == 500
        assert (
            response.get_json()["data"]["message"] == "Unable to execute result_reason"
        )
        mock_logger.exception.assert_called_once_with("Unable to execute result_reason")
