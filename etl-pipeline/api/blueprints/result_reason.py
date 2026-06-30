import json

import newrelic.agent
from flask import Blueprint, request
from openai import AsyncOpenAI

from logger import create_log, LogContextVars, get_app_logger
from utils.common import require_env
from utils.timer import timer

from ..auth import require_api_key
from ..decorators import require_session_jwt
from ..utils import APIUtils
from ..db import get_frbr_data_by_barcode
from ..assistant.agent import get_session_messages, format_frbr_fields
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


async def get_result_reason(
    session_id: str,
    call_id: str,
    barcode: str,
) -> tuple[str, bool]:
    """
    Generate an explanation for why a book (identified by barcode) appears in the
    search result identified by call_id.

    Returns (explanation, ai_generated) where ai_generated is False on fallback.
    """
    try:
        messages = get_session_messages(session_id)

        # TODO: if call_id does not exist in session messages, or the tool call output starts with error_prefix (btw centralize ERROR prefix in agent.py as a module var), return 404 (tool call output not found). with will handle the case of no session messages (right? what does get_session_messages return in that case). And separately return 404 if the barcode is not in the call_id tool call output (parse the XML)
        if tool_call_args is None:
            logger.warning(
                f"get_result_reason: call_id '{call_id}' not found in session '{session_id}'"
            )
            return FALLBACK_RESULT_REASON, False

        # TODO: truncate the conversation history (messages) to end after the tool call output with specified call id (before passing the messages to format_conversation_history)

        # Extract and format tool call args
        # TODO: look at sample data from the DB this parsing does not respect the actual data structure (which is in open ai response items format)
        tool_call_args = None
        for msg in messages:
            if msg.get("role") == "assistant":
                for tool_call in msg.get("tool_calls") or []:
                    if tool_call.get("id") == call_id:
                        tool_call_args = json.loads(tool_call["function"]["arguments"])
                        break
            if tool_call_args is not None:
                break
        formatted_tool_call_args = (
            f'Semantic query: "{tool_call_args.get("ranking_query", "")}"'
        )
        filters_raw = tool_call_args.get("filters")
        if filters_raw:
            formatted_tool_call_args += f"\nFilters: {filters_raw}"

        # format book details (from FRBR metadata)
        frbr_data = get_frbr_data_by_barcode([barcode])
        if frbr_data:
            row = frbr_data[0]
            frbr_fields = format_frbr_fields(row.Work, row.Edition)
            # TODO: this data extraction is duplicated in ..? (content search system prompt)
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
            model="gemini-3.5-flash",  # TODO: centralize as module var in agent.py DEFAULT_LLM
            messages=[
                {"role": "system", "content": system_prompt},
            ],
            temperature=0,
            reasoning_effort=None,
        )
        # TODO: handle if explanation=None, use fallback
        explanation = response.choices[0].message.content
        return explanation, True

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
    # Q: do the below need to be in every log, would it be enough just to log the session_id and log these once
    if call_id is not None:
        log_context["call_id"] = call_id
    if barcode is not None:
        log_context["barcode"] = barcode

    # New Relic Attributes
    # TODO: maybe consolidate into a function used here and in /chat
    for k, v in log_context.items():
        newrelic.agent.add_custom_attribute(k, v)
    if session_id:
        newrelic.agent.add_custom_attribute("llm.conversation_id", session_id)

    with LogContextVars(get_app_logger(), context=log_context):
        try:
            logger.info("Result reason request received")

            # Request Parameter Validation
            # TODO: turn individual param existence validation into a reusable function
            if not call_id:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "call_id is required"}
                )

            if not barcode:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "barcode is required"}
                )

            explanation, is_ai_generated = await get_result_reason(
                session_id, call_id, barcode
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
