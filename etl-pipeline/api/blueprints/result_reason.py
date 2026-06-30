import json

import newrelic.agent
from flask import Blueprint, request
from lxml import etree as ET
from openai import AsyncOpenAI

from logger import create_log, LogContextVars, get_app_logger
from utils.common import require_env
from utils.timer import timer

from ..auth import require_api_key
from ..decorators import require_session_jwt
from ..utils import APIUtils
from ..db import get_frbr_data_by_barcode
from ..assistant.agent import (
    DEFAULT_LLM,
    TOOL_ERROR_PREFIX,
    get_session_messages,
    format_frbr_fields,
)
from ..assistant.snippets import format_conversation_history


logger = create_log(__name__)

result_reason_blueprint = Blueprint("result_reason", __name__)

FALLBACK_RESULT_REASON = (
    "This result appears in your search based on its relevance to your query. "
    "The catalog's search algorithm identified this item as a potential match for your research. "
    "It may share themes, subjects, or content related to your inquiry."
)

RESULT_REASON_SYSTEM_PROMPT_TEMPLATE = """\
You are a research assistant at a library helping users understand why specific \
search results appear for their queries. Given the conversation history and the \
search query that was executed, explain in 3-4 sentences (~450 characters) why the \
specified book appears as a result. Be specific about what connects the book's \
subject matter, themes, or content to the user's research interest. Write clearly \
for a general audience.\

Conversation History:
{conversation_history}

The search query that returned these results was:
{query_description}

Explain why the following book appears in the results:
{book_info}

Write 3-4 sentences (~450 characters) explaining the connection between \
this book and the user's search.\
"""


class CallNotFoundError(Exception):
    """Raised when call_id or barcode cannot be resolved in the session."""

    pass


async def get_result_reason(
    session_id: str,
    call_id: str,
    barcode: str,
) -> tuple[str, bool]:
    """
    Generate an explanation for why a book (identified by barcode) appears in the
    search result identified by call_id.

    Returns (explanation, ai_generated) where ai_generated is False on fallback.
    Raises CallNotFoundError if session, call_id, or barcode cannot be resolved.
    """
    try:
        messages = get_session_messages(session_id)

        # --- 404 guard 1: session has no messages ---
        if not messages:
            logger.warning(
                f"get_result_reason: no messages found for session '{session_id}'"
            )
            raise CallNotFoundError(f"session '{session_id}' not found")

        # Find the raw function_call and function_call_output items for call_id.
        # Traverse in order so we know the truncation index after the output.
        function_call_item = None
        tool_call_output = None
        truncate_idx = None

        for i, msg in enumerate(messages):
            msg_type = msg.get("type")
            if msg_type == "function_call" and msg.get("call_id") == call_id:
                function_call_item = msg
            elif msg_type == "function_call_output" and msg.get("call_id") == call_id:
                tool_call_output = msg.get("output", "")
                truncate_idx = i + 1
                break

        # --- 404 guard 2: call_id not in session, or its output is a tool error ---
        if function_call_item is None or truncate_idx is None:
            logger.warning(
                f"get_result_reason: call_id '{call_id}' not found in session '{session_id}'"
            )
            raise CallNotFoundError(f"call_id '{call_id}' not found in session")

        if tool_call_output.startswith(TOOL_ERROR_PREFIX):
            logger.warning(
                f"get_result_reason: tool output for call_id '{call_id}' is an error"
            )
            raise CallNotFoundError(f"tool output for call_id '{call_id}' is an error")

        # --- 404 guard 3: barcode not present in the tool call output XML ---
        try:
            root = ET.fromstring(tool_call_output)
            barcodes_in_output = {el.text for el in root.findall(".//barcode")}
        except ET.XMLSyntaxError:
            barcodes_in_output = set()

        if barcode not in barcodes_in_output:
            logger.warning(
                f"get_result_reason: barcode '{barcode}' not found in tool output for call_id '{call_id}'"
            )
            raise CallNotFoundError(
                f"barcode '{barcode}' not found in results for call_id '{call_id}'"
            )

        # All 404 checks passed — now build query description and book info.

        # Truncate conversation history to end at (and including) this tool call output
        messages = messages[:truncate_idx]

        # Parse and format tool call args as a human-readable query description
        tool_call_args = json.loads(function_call_item.get("arguments", "{}"))
        formatted_tool_call_args = (
            f'Semantic query: "{tool_call_args.get("ranking_query", "")}"'
        )
        filters_raw = tool_call_args.get("filters")
        if filters_raw:
            formatted_tool_call_args += f"\nFilters: {filters_raw}"

        # Format book details (from FRBR metadata)
        frbr_data = get_frbr_data_by_barcode([barcode])
        if frbr_data:
            row = frbr_data[0]
            frbr_fields = format_frbr_fields(row.Work, row.Edition)
            book_info = (
                f"Title: {frbr_fields['title']}\n"
                f"Authors: {frbr_fields['author_names']}\n"
                f"Subjects: {frbr_fields['subject_list']}\n"
                f"Publication date: {frbr_fields['pub_date']}"
            )
        else:
            logger.warning(
                f"get_result_reason: no FRBR data found for barcode '{barcode}'"
            )
            book_info = f"(Book metadata unavailable for barcode: {barcode})"

        conversation_history = format_conversation_history(messages)

        # TODO: find all the places I make an LLM call and make a centralized wrapper call_google_llm(model=, messages=, **kwargs<passed to completion.create()>)
        client = AsyncOpenAI(
            api_key=require_env("GOOGLE_API_KEY"),
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )

        system_prompt = RESULT_REASON_SYSTEM_PROMPT_TEMPLATE.format(
            conversation_history=conversation_history,
            query_description=formatted_tool_call_args,
            book_info=book_info,
        )

        response = await client.chat.completions.create(
            model=DEFAULT_LLM,
            messages=[
                {"role": "system", "content": system_prompt},
            ],
            temperature=0,
            reasoning_effort=None,
        )
        explanation = response.choices[0].message.content
        if explanation is None:
            logger.warning(
                "get_result_reason: LLM returned None content, using fallback"
            )
            return FALLBACK_RESULT_REASON, False

        return explanation, True

    except CallNotFoundError:
        raise
    except Exception:
        logger.exception(
            "get_result_reason: failed to generate explanation, using fallback"
        )
        return FALLBACK_RESULT_REASON, False


@result_reason_blueprint.route("/result-reason", methods=["POST"])
@require_api_key
@require_session_jwt
@timer(logger)
async def result_reason(session_id):
    response_type = "result_reason"

    call_id = request.json.get("call_id")
    barcode = request.json.get("barcode")

    log_context = {"session_id": session_id}
    if call_id is not None:
        log_context["call_id"] = call_id
    if barcode is not None:
        log_context["barcode"] = barcode

    # TODO: maybe consolidate into a function used here and in /chat
    for k, v in log_context.items():
        newrelic.agent.add_custom_attribute(k, v)
    if session_id:
        newrelic.agent.add_custom_attribute("llm.conversation_id", session_id)

    with LogContextVars(get_app_logger(), context=log_context):
        try:
            logger.info("Result reason request received")

            # TODO: turn individual param existence validation into a reusable function
            if not call_id:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "call_id is required"}
                )

            if not barcode:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "barcode is required"}
                )

            try:
                explanation, is_ai_generated = await get_result_reason(
                    session_id, call_id, barcode
                )
            except CallNotFoundError as e:
                return APIUtils.formatResponseObject(
                    404, response_type, {"message": str(e)}
                )

            response_data = {
                "explanation": explanation,
                "is_ai_generated": is_ai_generated,
                "session_id": session_id,
            }
            return APIUtils.formatResponseObject(200, response_type, response_data)

        except Exception:
            logger.exception("Unable to execute result_reason")
            return APIUtils.formatResponseObject(
                500, response_type, {"message": "Unable to execute result_reason"}
            )
