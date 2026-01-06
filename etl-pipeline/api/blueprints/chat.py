from flask import Blueprint, current_app, request

# shared code
from logger import create_log

# API code
from ..utils import APIUtils, shorten
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_token
from ..assistant.agent import update_chat, AssistantWorkerContext, PAGE_SIZE


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
    conversation_type = request.json.get("conversation_type")
    conversation = request.json.get("messages")
    item_id = request.json.get("itemId")

    # Input parameter validation
    assert conversation_type in ["contentSearch", "catalogSearch"]
    if conversation_type == "contentSearch":
        assert item_id is not None, (
            'item_id is required for conversation_type="contentSearch"'
        )

    run_result = update_chat(conversation, conversation_type, item_id=item_id)

    ## Build API response

    # Extract new messages in conversation
    messages = [item.to_input_item() for item in run_result.new_items]

    # Extract (single) search tool result
    search_results = run_result.context_wrapper.context.search_results
    if len(search_results) > 1:
        logger.warning(
            f"{len(len(conversation_type.search_results))} tool calls during agent response."
        )
    search_result = list(search_results.values())[0]
    # TODO: if multiple search results/tool calls per update... merge into a single result

    # Format search result for API response
    if conversation_type == "catalogSearch":
        formatted_search_result = {
            "editions": ...,  # search_result ... # edition tup to dict
            "search_params": search_result["search_params"],
            # NOTE: paginated search not yet implemented, only 1 fixed result set size
            "paging": APIUtils.formatPagingOptions(
                page=1, pageSize=PAGE_SIZE, totalHits=len(search_result["edition_data"])
            ),
        }

    else:  # contentSearch
        formatted_search_result = {
            # TODO: extract snippets intelligently
            # TODO: map the start/end pages to the snippets
            "snippets": [shorten(h["text"]) for h in search_result["chunk_hits"]],
            "search_params": search_result["search_params"],
        }

    response_data = {
        "messages": messages,
        "result": formatted_search_result,
    }
    return APIUtils.formatResponseObject(200, RESPONSE_TYPE, response_data)
