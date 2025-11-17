import json
import random
import time

from flask import Blueprint, Response, current_app, request, stream_with_context
from logger import create_log

from ..auth import require_api_key
from ..db import DBClient
from ..decorators import require_token
from ..elastic import ElasticClient
from ..research_assistant import ResearchAssistant
from ..utils import APIUtils

logger = create_log(__name__)

chats_blueprint = Blueprint("chats", __name__, url_prefix="/chats")
RESPONSE_TYPE = "chats"


@chats_blueprint.route("", methods=["PUT"])
@require_api_key
@require_token
def update_chat(user=None):
    research_assistant = ResearchAssistant(
        ElasticClient(current_app.config["REDIS_CLIENT"]),
        DBClient(current_app.config["DB_CLIENT"]),
    )
    messages = request.json.get("messages")

    if not messages:
        return APIUtils.formatResponseObject(
            400, RESPONSE_TYPE, {"message": f"Chat request is missing messages"}
        )

    response = research_assistant.get_chat_completion(messages)

    return APIUtils.formatResponseObject(201, RESPONSE_TYPE, response)


# TODO: Implement @require_api_key and @require_token
@chats_blueprint.route("/stream", methods=["POST"])
def update_chat_stream():
    messages = request.json.get("messages")

    if not messages:
        return APIUtils.formatResponseObject(
            400, "chat", {"message": "Chat request is missing messages"}
        )

    # mock delay before sending response
    time.sleep(1.5)

    def generate():
        mock_response = _generate_mock_response()
        for chunk in mock_response:
            line = json.dumps(
                {"status": 200, "type": "chat", "data": {"message": chunk}}
            )
            yield line + "\n"  # NDJSON is newline-delimited
            # mock delays between chunks sent to the frontend
            time.sleep(0.01)

    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "X-Accel-Buffering": "no",  # when buffering is enabled, the response will be buffered and not sent immediately
        "Connection": "keep-alive",
    }

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",  # alternative: "text/event-stream" for SSE
        headers=headers,
    )


def _generate_mock_response():
    return (
        'I searched the catalog for "history of new york" and found the following items:'
        ' * "Natural history of New York" by New York (State). Natural History Survey.'
        ' * "Charter, constitution, and by-laws of the Lyceum of Natural History in the city of New-York" by New York Academy of Sciences.'
        ' * "Proceedings of the Lyceum of Natural History in the City of New York." by Lyceum of Natural History (New York, N.Y.).'
        ' * "New York history." by New York State Historical Association.'
        ' * "A history of New York" by Knickerbocker, D. (1783-1859.)'
        ' * "History of the city of New York" by Lamb, M. J. ()'
        ' * "The Documentary history of the state of New-York" by O\'Callaghan, Edmund Bailey, (1797-1880.)'
    )
