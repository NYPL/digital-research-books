from flask import Blueprint, current_app, request

# shared code
from logger import create_log

# API code
from ..utils import APIUtils
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_token
from ..assistant.agent import update_chat, AssistantWorkerContext


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")

RESPONSE_TYPE = "chat"
INDEX_NAME = "vra_chunks_gemini-embedding-001"
WORKER_CONTEXT = AssistantWorkerContext(
    index_name=INDEX_NAME
)  # Q: is there a more flask idomatic way to do this?


@chat_blueprint.route("/", methods=["POST"])
@require_api_key
@require_token
def chat(user=None):
    conversation_type = request.json.get("context")
    conversation = request.json.get("messages")
    item_id = request.json.get("itemId")

    with DBClient(current_app.config["DB_CLIENT"]) as db_client:
        response_data = update_chat(conversation, conversation_type, item_id=item_id)

        return APIUtils.formatResponseObject(200, RESPONSE_TYPE, response_data)
