from textwrap import indent
from flask import Blueprint, current_app, request

# shared code
from logger import create_log
from model.postgres.edition import Edition
from model.postgres.item import Item
from model.postgres.link import Link
from model.postgres.rights import Rights
from model.postgres.work import Work

# API code
from ..utils import APIUtils, orm_to_dict, shorten
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_basic_authentication
from ..assistant.agent import update_chat, PAGE_SIZE


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")

RESPONSE_TYPE = "chat"


# TODO: Q: extract chunk hits here or in agent.py in search tool? any way to concurrently \
# extract relevant snippets while LLM is finishing response?
# TODO: handle ordering of relevant snippets!!!
# TODO: extract snippets intelligently
# MAYBE: limit the number of returned snippets
def get_relevant_snippets(chunk_hits):
    return [
        {
            "text": shorten(h["text"]),
            "start_page": h["chunk_start_page"],
            "end_page": h["chunk_end_page"],
            "item_id": h["item_id"],
            "chunk_score": h["meta"]["score"],
        }
        for h in chunk_hits
    ]


def format_search_results(search_results):
    """
    Convert dict of tool_call_id->results into an output formatted for
    frontend consumption.
    Expects len(search_results)==1
    """

    # Extract (single) search tool result
    if len(search_results) > 1:
        # TODO: handle if multiple search results/tool calls per update... merge into a single result
        logger.error(f"Agent recorded {len(search_results)} search tool results")
        search_result = list(search_results.values())[-1]
    elif len(search_results) == 1:
        search_result = list(search_results.values())[0]
    else:
        # TODO: handle no search result (i.e. the agent didn't update the search results (also no pagination))
        search_result = None

    # Format search result for API response
    if search_result:
        if search_result["tool_name"] == "search_library_catalog":
            editions = []
            for edition_datum in search_result["edition_data"]:
                # FRBR ORM to dict
                work_dict = orm_to_dict(
                    edition_datum["orm_work"],
                    exclude=[(Work, "date_created"), (Work, "date_modified")],
                )
                # prepend "work_" to work fields
                work_dict = {f"work_{k}": v for k, v in work_dict.items()}
                edition_dict = orm_to_dict(
                    edition_datum["orm_edition"],
                    exclude=[
                        (Edition, "date_created"),
                        (Edition, "date_modified"),
                        (Edition, "dcdw_uuids"),
                        (Item, "date_created"),
                        (Item, "date_modified"),
                        (Item, "modified"),
                        (Item, "publisher_project_source"),
                        (Item, "record_id"),
                        (Link, "date_created"),
                        (Link, "date_modified"),
                        (Link, "md5"),
                        (Rights, "date_created"),
                        (Rights, "date_created"),
                        (Rights, "date_modified"),
                        (Rights, "id"),
                        (Rights, "rights_date"),
                        (Rights, "rights_reason"),
                    ],
                )
                # Q: should we be including the chunk_hits/snippet in editions or top level?
                edition_dict.update(
                    {
                        **work_dict,
                        "snippets": get_relevant_snippets(
                            edition_datum["edition_hit"]["chunk_hits"]
                        ),
                    }
                )
                editions.append(edition_dict)

            result_type = ("catalogSearch",)  # MAYBE: send search tool name
            formatted_search_result = {
                "editions": editions,
                "search_params": search_result["search_params"],
                # NOTE: paginated search not yet implemented, only 1 fixed result set size
                "paging": APIUtils.formatPagingOptions(
                    page=1,
                    pageSize=PAGE_SIZE,
                    totalHits=len(editions),
                ),
            }
            logger.info(
                f"Returning {len(editions)} editions in catalog search response"
            )  # Q: redundant to tool call logging

        elif search_result["tool_name"] == "search_in_book":
            result_type = ("contentSearch",)  # MAYBE: send search tool name
            formatted_search_result = {
                "snippets": get_relevant_snippets(search_result["chunk_hits"]),
                "search_params": search_result["search_params"],
            }
            logger.info(
                f"Returning {len(search_result['chunk_hits'])} snippets in content search response"
            )
        else:
            raise ValueError(
                f"Unsupported search tool type: {search_result['tool_name']}"
            )
    else:
        result_type = None
        formatted_search_result = None
        logger.info(
            "No search results to return (agent did record search tool call result)"
        )

    return result_type, formatted_search_result


@chat_blueprint.route("/", methods=["POST"])
@require_api_key
@require_basic_authentication
def chat(user=None):
    conversation_type = request.json.get("conversationType")
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

    # Format search results
    result_type, formatted_search_result = format_search_results(
        run_result.context_wrapper.context.search_results
    )

    response_data = {
        "messages": messages,
        "result_type": result_type,
        "result": formatted_search_result,
    }
    return APIUtils.formatResponseObject(200, RESPONSE_TYPE, response_data)
