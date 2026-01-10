from flask import Blueprint, current_app, request

# shared code
from logger import create_log

# API code
from ..utils import APIUtils, orm_to_dict, shorten
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_token
from ..assistant.agent import update_chat, PAGE_SIZE


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")

RESPONSE_TYPE = "chat"


# TODO: Q: extract chunk hits here or in agent.py in search tool? any way to concurrently \
# extract relevant snippets while LLM is finishing response?
# TODO: handle ordering of relevant snippets!!!
def get_relevant_snippets(chunk_hits):
    return [
        {
            "text": shorten(h["text"]),
            "start_page": h["chunk_start_page"],
            "end_page": h["chunk_end_page"],
        }
        for h in chunk_hits
    ]


@chat_blueprint.route("/", methods=["POST"])
@require_api_key
# @require_token
def chat(user=None):
    conversation_type = request.json.get("conversation_type")
    conversation = request.json.get("messages")
    edition_id = request.json.get("editionId")

    logger.info(
        f"Chat request received: conversation_type={conversation_type}, edition_id={edition_id}, messages_count={len(conversation) if conversation else 0}"
    )

    # Input parameter validation
    assert conversation_type in ["contentSearch", "catalogSearch"]
    if conversation_type == "contentSearch":
        assert edition_id is not None, (
            'edition_id is required for conversation_type="contentSearch"'
        )

    # get LLM response + search results
    run_result = update_chat(conversation, conversation_type, edition_id=edition_id)
    # TODO: when a search tool errors it is handled the LLM responds (ussually saying sorry I had an error) and a 200 response is returned

    ## Build API response
    # Extract new messages
    messages = [item.to_input_item() for item in run_result.new_items]
    logger.info(f"Agent generated {len(run_result.new_items)} new message items")

    # Extract (single) search tool result
    search_results = run_result.context_wrapper.context.search_results
    logger.info(f"Agent recorded {len(search_results)} search tool results")
    if len(search_results) > 1:
        # TODO: handle if multiple search results/tool calls per update... merge into a single result
        logger.warning(
            f"{len(len(conversation_type.search_results))} tool calls during agent response."
        )
    elif len(search_results) == 1:
        search_result = list(search_results.values())[0]
    else:
        search_result = None
    # TODO: handle no search result (i.e. the agent didn't update the search results (also no pagination))

    # Format search result for API response
    if search_result:
        if conversation_type == "catalogSearch":
            # FRBR ORM to dict
            work_dict = orm_to_dict(search_result["edition_data"][0])
            work_dict = {
                f"work.{k}": v for k, v in work_dict.items()
            }  # prepend "work." to work fields
            edition_dict = orm_to_dict(search_result["edition_data"][1])
            # Q: should we be including the chunk_hits/snippet in editions or top level?
            edition_dict.update(
                {
                    **work_dict,
                    "snippets": get_relevant_snippets(search_result[2]["chunk_hits"]),
                }
            )

            formatted_search_result = {
                "editions": edition_dict,
                # TODO: add relevant snippets for editions
                "search_params": search_result["search_params"],
                # NOTE: paginated search not yet implemented, only 1 fixed result set size
                "paging": APIUtils.formatPagingOptions(
                    page=1,
                    pageSize=PAGE_SIZE,
                    totalHits=len(search_result["edition_data"]),
                ),
            }
            logger.info(
                f"Returning {len(search_result['edition_data'])} editions in catalog search response"
            )  # Q: redundant to tool call logging

        else:  # contentSearch
            formatted_search_result = {
                # TODO: extract snippets intelligently
                # MAYBE: limit the number of returned snippets
                # TODO: map the start/end pages to the snippets
                "snippets": get_relevant_snippets(search_result["chunk_hits"]),
                "search_params": search_result["search_params"],
            }
            logger.info(
                f"Returning {len(search_result['chunk_hits'])} snippets in content search response"
            )
    else:
        formatted_search_result = {}
        logger.info(
            "No search results to return (agent did record search tool call result)"
        )

    response_data = {
        "messages": messages,
        "result": formatted_search_result,
    }
    return APIUtils.formatResponseObject(200, RESPONSE_TYPE, response_data)
