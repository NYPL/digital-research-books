# builtins
import asyncio
from dataclasses import asdict
from typing import Dict, Tuple

# non-built-ins
from flask import Blueprint, request
import newrelic.agent

# shared code
from logger import create_log, LogContextVars, get_app_logger
from model.postgres.edition import Edition
from model.postgres.item import Item
from model.postgres.link import Link
from model.postgres.rights import Rights
from model.postgres.work import Work
from utils.timer import timer

# API code
from ..utils import APIUtils, orm_to_dict
from ..assistant.session import JSONBSQLAlchemySession

from ..db import DBClient, get_async_engine
from ..auth import require_api_key
from ..decorators import require_session_jwt
from ..assistant.agent import (
    SCORE_SORT_DIRECTION,
    BookNotFoundError,
    update_chat,
    PAGE_SIZE,
    get_max_message_id,
    get_session_messages_after,
    get_new_items_with_ids,
)
from ..assistant.snippets import get_relevant_snippets


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")


def prepare_search_response(search_results) -> Tuple[str, Dict] | Tuple[None, None]:
    """
    Extract final search result, prepare structure for expected chat/ response,
    and convert to serializable types.

    Returns (result_type, formatted_search_result)
    Returns (None, None) if search_results is empty (no search was executed)
    `formatted_search_result` is the search results is consumable by flask.jsonify.
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
        logger.info(
            "No search results to return (agent did record search tool call result)"
        )

    # Build search result for API response
    result_type = None
    formatted_search_result = None
    if search_result:
        # Extract search tool type
        result_type = (
            "catalogSearch"
            if search_result["tool_name"] == "search_catalog"
            else "contentSearch"
            if search_result["tool_name"] == "search_book"
            else None
        )
        if result_type is None:
            raise ValueError(
                f"Unsupported search tool type: {search_result['tool_name']}"
            )

        search_params = search_result["search_params"]

        # Format edition-level response data
        editions = []
        for edition_result in search_result["edition_data"]:
            edition_response = {}

            # Add snippets to edition response
            # Sort editions + convert to json serializable form
            edition_response["snippets"] = [
                asdict(s)
                for s in sorted(
                    edition_result.snippets,
                    key=lambda s: s.chunk_score,
                    **SCORE_SORT_DIRECTION,
                )
            ]

            if result_type == "catalogSearch":
                # Add FRBR metadata to edition response

                edition_metadata = orm_to_dict(
                    edition_result.orm_edition,
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
                    column_formatters={
                        "publication_date": (lambda d: d.year if d else None)
                    },
                )
                work_metadata = orm_to_dict(
                    edition_result.orm_work,
                    exclude=[(Work, "date_created"), (Work, "date_modified")],
                )
                # prepend "work_" to work fields
                work_metadata = {f"work_{k}": v for k, v in work_metadata.items()}

                edition_response.update({**edition_metadata, **work_metadata})

            editions.append(edition_response)

        # Format search result response
        if result_type == "catalogSearch":
            formatted_search_result = {
                "editions": editions,
                "search_params": search_params,
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

        else:  # result_type == "contentSearch"
            snippets = editions[0]["snippets"]
            formatted_search_result = {
                "snippets": snippets,
                "search_params": search_params,
            }
            logger.info(
                f"Returning {len(snippets)} snippets in content search response"
            )

    return result_type, formatted_search_result


@chat_blueprint.route("", methods=["POST"])
@require_api_key
@require_session_jwt
@timer(logger)
def chat(session_id):
    response_type = "chat"

    conversation_type = request.json.get("conversationType")
    message = request.json.get("message")
    edition_id = request.json.get("editionId")
    barcode = request.json.get("barcode")

    log_context = {"session_id": session_id, "conversation_type": conversation_type}
    if edition_id is not None:
        log_context["edition_id"] = edition_id
    if barcode is not None:
        log_context["barcode"] = barcode

    # New Relic custom attributes and AI monitoring conversation grouping
    for k, v in log_context.items():
        newrelic.agent.add_custom_attribute(k, v)
    if session_id:
        newrelic.agent.add_custom_attribute("llm.conversation_id", session_id)

    # TODO: switch to a setup where you can add and remove log context vars inside \
    # the log context vars context while scoping the context to the entire view \
    # function. This allows the 500 error catch all log to get context vars if \
    # available while also starting from the very top of the view function or \
    # even being a global error handler with logger defined in a different module. \
    # something like https://www.structlog.org/en/stable/contextvars.html
    with LogContextVars(get_app_logger(), context=log_context):
        try:
            logger.info(f"Chat request received: {message[:20]}...")

            if not message:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "message is required"}
                )

            if not conversation_type:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "conversationType is required"}
                )

            if conversation_type not in ["contentSearch", "catalogSearch"]:
                return APIUtils.formatResponseObject(
                    400,
                    response_type,
                    {
                        "message": "conversationType must be either 'contentSearch' or 'catalogSearch'"
                    },
                )

            if (
                conversation_type == "contentSearch"
                and edition_id is None
                and barcode is None
            ):
                return APIUtils.formatResponseObject(
                    400,
                    response_type,
                    {
                        "message": "editionId or barcode is required for conversationType='contentSearch'"
                    },
                )

            # get LLM response + search results
            # TODO: inside update_chat make sure than any errors are handled by a polite \
            # llm generated response (except no connectivity to LLM) (just handle the \
            # high level openai agents sdk errors)
            session = JSONBSQLAlchemySession(session_id, engine=get_async_engine())
            max_id = get_max_message_id()
            try:
                run_result = asyncio.run(
                    update_chat(
                        message,
                        conversation_type,
                        edition_id=edition_id,
                        barcode=barcode,
                        session=session,
                    )
                )
            except BookNotFoundError as e:
                return APIUtils.formatResponseObject(
                    404, response_type, {"message": str(e)}
                )

            session_message_items = get_session_messages_after(session_id, max_id)

            # Add relevant snippets to search result, if search was executed in this agent turn
            # snippets updated in run_result in place
            asyncio.run(get_relevant_snippets(run_result, approach="naive"))

            ## Build API response

            # Note: Concurrent writes to *this* session are theoretically possible but unlikely
            # in practice; get_new_items_with_ids() guards against them by only returning
            # items that also appear in RunResult.new_items. MAYBE: just use session_message_items without filtering?
            messages = get_new_items_with_ids(run_result, session_message_items)
            logger.info(f"Agent generated {len(messages)} new message items")

            # Format search results
            result_type, formatted_search_result = prepare_search_response(
                run_result.context_wrapper.context.search_results
            )

            response_data = {
                "messages": messages,
                "result_type": result_type,
                "result": formatted_search_result,
                "session_id": session_id,
            }
            return APIUtils.formatResponseObject(200, response_type, response_data)

        except Exception:
            logger.exception("Unable to execute chat")
            return APIUtils.formatResponseObject(
                500, response_type, {"message": "Unable to execute chat"}
            )
