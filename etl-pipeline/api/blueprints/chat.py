import asyncio
from dataclasses import asdict
import queue
import threading
from textwrap import indent
from typing import Any, Dict, Tuple
from api.assistant.types import CatalogSearchResult
from flask import Blueprint, current_app, request, Response, stream_with_context
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
from ..elastic import ElasticClient
from ..db import DBClient
from ..auth import require_api_key
from ..decorators import require_session_jwt
from ..assistant.agent import SCORE_SORT_DIRECTION, update_chat, PAGE_SIZE
from ..assistant.snippets import get_relevant_snippets
from ..assistant.streaming_utils import (
    serialize_event,
    format_final_response,
    format_error,
)


logger = create_log(__name__)

chat_blueprint = Blueprint("chat", __name__, url_prefix="/chat")

RESPONSE_TYPE = "chat"


def prepare_search_response(search_results) -> Tuple[str, Dict] | Tuple[None, None]:
    """
    Extract final search result, prepare structure for expected chat/ response,
    and convert to serializable types.

    Returns (result_type, formatted_search_result) where formatted_search_result
    is the search results serialized for http response via flask.jsonify
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

    # Format search result for API response

    result_type = None
    formatted_search_result = None
    if search_result:
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

        editions = []
        for edition_result in search_result["edition_data"]:
            edition = {}

            # Sort editions + convert to json serializable form
            edition["snippets"] = [
                asdict(s)
                for s in sorted(
                    edition_result.snippets,
                    key=lambda s: s.chunk_score,
                    **SCORE_SORT_DIRECTION,
                )
            ]

            if result_type == "catalogSearch":
                # FRBR ORM to dict

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

                edition.update({**edition_metadata, **work_metadata})

            editions.append(edition)

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

    with LogContextVars(get_app_logger(), context=log_context):

        def generate_streaming_response():
            try:
                yield from _chat_stream_handler(
                    session_id, conversation_type, message, edition_id, barcode
                )
            except Exception as e:
                logger.exception("Error in streaming chat handler")
                yield format_error(str(e), code="stream_error")

        return Response(
            stream_with_context(generate_streaming_response()),
            mimetype="application/x-ndjson",
            headers={"Transfer-Encoding": "chunked"},
        )


def _chat_stream_handler(session_id, conversation_type, message, edition_id, barcode):
    """
    Generator that yields NDJSON events during chat processing.
    Validates input, processes chat, and yields progress/result events.
    """
    return _chat_handler(session_id, conversation_type, message, edition_id, barcode)


def _chat_handler(session_id, conversation_type, message, edition_id, barcode):
    """wrapper for main chat() logic to allow use of LogContextVars without a huge indent block"""

    logger.info(f"Chat request received: {message[:20]}...")

    if not message:
        yield format_error("message is required", code="validation_error")
        return

    if not conversation_type:
        yield format_error("conversationType is required", code="validation_error")
        return

    if conversation_type not in ["contentSearch", "catalogSearch"]:
        yield format_error(
            "conversationType must be either 'contentSearch' or 'catalogSearch'",
            code="validation_error",
        )
        return

    if conversation_type == "contentSearch" and edition_id is None and barcode is None:
        yield format_error(
            "editionId or barcode is required for conversationType='contentSearch'",
            code="validation_error",
        )
        return

    logger.info(f"Streaming chat request received: {message[:20]}...")

    try:
        event_queue: queue.Queue = queue.Queue()
        completion_marker = object()
        run_result_holder = {"result": None, "error": None}

        def on_event(event_type: str, payload: Dict[str, Any]) -> None:
            event_queue.put(serialize_event(event_type, payload))

        def run_chat() -> None:
            try:
                run_result_holder["result"] = asyncio.run(
                    update_chat(
                        message,
                        conversation_type,
                        session_id,
                        edition_id=edition_id,
                        barcode=barcode,
                        event_callback=on_event,
                    )
                )
            except Exception as exc:
                run_result_holder["error"] = exc
            finally:
                event_queue.put(completion_marker)

        worker = threading.Thread(target=run_chat, daemon=True)
        worker.start()

        while True:
            event = event_queue.get()
            if event is completion_marker:
                break
            yield event

        if run_result_holder["error"] is not None:
            raise run_result_holder["error"]

        run_result = run_result_holder["result"]

        # Add relevant snippets to search result, if search was executed in this agent turn
        # snippets updated in run_result in place
        asyncio.run(get_relevant_snippets(run_result, approach="naive"))

        # Extract new messages
        messages = [item.to_input_item() for item in run_result.new_items]
        logger.info(f"Agent generated {len(run_result.new_items)} new message items")

        # Format search results
        result_type, formatted_search_result = prepare_search_response(
            run_result.context_wrapper.context.search_results
        )

        # Emit final response event
        yield format_final_response(
            messages=messages,
            result_type=result_type,
            result=formatted_search_result,
            session_id=session_id,
        )

    except Exception as e:
        logger.exception("Error processing streaming chat request")
        yield format_error(str(e), code="processing_error")
