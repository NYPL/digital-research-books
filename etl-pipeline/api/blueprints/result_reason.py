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
from ..assistant.agent import (
    DEFAULT_LLM,
    TOOL_ERROR_PREFIX,
    get_session_messages,
)
from ..assistant.snippets import format_conversation_history


logger = create_log(__name__)

result_reason_blueprint = Blueprint("result_reason", __name__)

FALLBACK_RESULT_REASON = (
    "This result appears in your search based on its relevance to your query. "
    "The catalog's search algorithm identified this item as a potential match for your research. "
    "It may share themes, subjects, or content related to your inquiry."
)

# TODO: add simple prose paragraph formatting instruction
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
{item_result}

Write 3-4 sentences (~450 characters) explaining the connection between \
this book and the user's search.\
"""


class CallNotFoundError(Exception):
    """Raised when call_id or barcode cannot be resolved in the session."""

    pass


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
                try:
                    messages = get_session_messages(session_id)

                    # --- 404 guard 1: session has no messages ---
                    if not messages:
                        logger.warning(
                            f"get_result_reason: no messages found for session '{session_id}'"
                        )
                        raise CallNotFoundError(f"session '{session_id}' not found")

                    # Find the raw function_call and function_call_output items for call_id.
                    # Traverse in order so we know the truncation index after the call's arguments.
                    function_call_item = None
                    tool_call_output = None
                    truncate_idx = None

                    for i, msg in enumerate(messages):
                        msg_type = msg.get("type")
                        if (
                            msg_type == "function_call"
                            and msg.get("call_id") == call_id
                        ):
                            function_call_item = msg
                            truncate_idx = i + 1
                        elif (
                            msg_type == "function_call_output"
                            and msg.get("call_id") == call_id
                        ):
                            tool_call_output = msg.get("output", "")
                            break

                    # --- 404 guard 2: call_id not in session, or its output is a tool error ---
                    if function_call_item is None or tool_call_output is None:
                        logger.warning(
                            f"get_result_reason: call_id '{call_id}' not found in session '{session_id}'"
                        )
                        raise CallNotFoundError(
                            f"call_id '{call_id}' not found in session"
                        )

                    if tool_call_output.startswith(TOOL_ERROR_PREFIX):
                        logger.warning(
                            f"get_result_reason: tool output for call_id '{call_id}' is an error"
                        )
                        raise CallNotFoundError(
                            f"tool output for call_id '{call_id}' is an error"
                        )

                    # --- 404 guard 3: barcode not present in the tool call output XML ---
                    try:
                        root = ET.fromstring(tool_call_output)
                        editions_in_output = root.findall("edition")
                    except ET.XMLSyntaxError:
                        editions_in_output = []

                    item_result_el = next(
                        (
                            el
                            for el in editions_in_output
                            if el.findtext("barcode") == barcode
                        ),
                        None,
                    )

                    if item_result_el is None:
                        logger.warning(
                            f"get_result_reason: barcode '{barcode}' not found in tool output for call_id '{call_id}'"
                        )
                        raise CallNotFoundError(
                            f"barcode '{barcode}' not found in results for call_id '{call_id}'"
                        )

                    # All 404 checks passed — now build query description and item result.

                    # Truncate conversation history to end at (and including) this tool call's
                    # arguments, excluding the tool call output.
                    messages = messages[:truncate_idx]

                    # Parse and format tool call args as a human-readable query description
                    tool_call_args = json.loads(
                        function_call_item.get("arguments", "{}")
                    )
                    formatted_tool_call_args = (
                        f'Semantic query: "{tool_call_args.get("ranking_query", "")}"'
                    )
                    filters_raw = tool_call_args.get("filters")
                    if filters_raw:
                        formatted_tool_call_args += f"\nFilters: {filters_raw}"

                    # Extract the full <edition> result for this barcode from the search results
                    item_result = ET.tostring(
                        item_result_el, encoding="unicode"
                    ).strip()

                    conversation_history = format_conversation_history(messages)

                    # TODO: find all the places I make an LLM call and make a centralized wrapper call_google_llm(model=, messages=, **kwargs<passed to completion.create()>)
                    # TODO: use sync OpenaiClient
                    client = AsyncOpenAI(
                        api_key=require_env("GOOGLE_API_KEY"),
                        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                    )

                    system_prompt = RESULT_REASON_SYSTEM_PROMPT_TEMPLATE.format(
                        conversation_history=conversation_history,
                        query_description=formatted_tool_call_args,
                        item_result=item_result,
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
                        explanation, is_ai_generated = FALLBACK_RESULT_REASON, False
                    else:
                        explanation, is_ai_generated = explanation, True

                except CallNotFoundError:
                    raise
                except Exception:
                    logger.exception(
                        "get_result_reason: failed to generate explanation, using fallback"
                    )
                    explanation, is_ai_generated = FALLBACK_RESULT_REASON, False
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
