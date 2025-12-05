from flask import Blueprint, current_app, request
from ..elastic import ElasticClient
from ..db import DBClient

from ..utils import APIUtils
from logger import create_log
from ..research_assistant import ResearchAssistant
from ..auth import require_api_key
from ..decorators import require_token

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
    initial_message_type = request.json.get("initialMessageType", "vra")

    if not messages:
        initial_message = research_assistant.get_initial_message(initial_message_type)
        return APIUtils.formatResponseObject(200, RESPONSE_TYPE, initial_message)

    response = research_assistant.get_chat_completion(messages)

    return APIUtils.formatResponseObject(201, RESPONSE_TYPE, response)
