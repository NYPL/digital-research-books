import json

import newrelic.agent
from flask import Blueprint, request
from openai import OpenAI

from logger import create_log, LogContextVars, get_app_logger
from utils.common import require_env
from utils.timer import timer

from ..auth import require_api_key
from ..decorators import require_session_jwt
from ..utils import APIUtils
from ..assistant.agent import (
    DEFAULT_LLM,
    TOOL_ERROR_PREFIX,
    find_result_by_edition_id,
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

RESULT_REASON_SYSTEM_PROMPT_TEMPLATE = """\
You are a research assistant at a library helping users understand why specific \
search results appear for their queries. Given the conversation history and the \
search query that was executed, explain in 3-4 sentences (~450 characters) why the \
specified book appears as a result. Be specific about what connects the book's \
subject matter, themes, or content to the user's research interest. Write clearly \
for a general audience.\

Conversation History:
{conversation_history}

The final search query is the one that returned the book that whose presence you \
must explain.

Here is the full result for the the book whose presence in the results you must \
explain. The result includes some book metadata and the text chunks in the book \
that best matched the search query:
{edition_result}

Write 3-4 sentences or ~400 characters (whichever is less) explaining the \
connection between this book and the user's search.

Closest match results are always returned even if there are no truly relevant \
matches in our search catalog. If the book is truly not relevant to the user's \
query, tell the user it's the closest match even though it isn't really relevant \
to their query. Include a very short hypothesis about why the irrelevant results \
might have been returned (e.g., matching a shared first name but wrong entity).

Format your response as standard, flowing paragraph prose without any markdown. 
Use plain text and italics for emphasis. No other markdown, syntax, or HTML is permitted.
* Do NOT use list structures of any kind (no bullets *, -, •, or numbered lists).
* Do NOT use structural Markdown headers (#, ##, ###) within the body of your response.
* Do NOT use links, code blocks, or inline code backticks.
"""


def get_tool_call_by_id(messages, call_id):
    """Return tool call output by tool call_id lookup.

    Args:
        messages: list of responses api items containing the conversation history.
        call_id: Desired function call_id

    Returns: (args, output, idx) for the function call, where idx is the index
    of the function call args item.

    None is returned for all if function call does not exist in conversation history.
    """
    # Find the raw function_call and function_call_output items for call_id.
    # Traverse in order so we know the truncation index after the call's arguments.
    function_call_args = None
    function_call_output = None
    function_call_idx = None

    for i, msg in enumerate(messages):
        msg_type = msg.get("type")
        if msg_type == "function_call" and msg.get("call_id") == call_id:
            function_call_args = msg
            function_call_idx = i + 1
        elif msg_type == "function_call_output" and msg.get("call_id") == call_id:
            function_call_output = msg.get("output", "")
            break
    return function_call_args, function_call_output, function_call_idx


def get_result_reason(messages, edition_result):
    """Make LLM call to generate "why am I seeing this result?" explanation.

    Args:
        messages: conversation history truncated to end at (and including) the
        tool call's arguments, excluding the tool call output.
        edition_result: the specific edition result whose presence must be explained.

    Returns: (explanation, is_ai_generated)
    """
    # MAYBE: should the testable boundary around LLM generation be even more
    # generic and just take the input items/messages?
    conversation_history = format_conversation_history(messages)

    # MAYBE: find all the places I make an LLM call and make a centralized wrapper call_google_llm(model=, messages=, **kwargs<passed to completion.create()>)
    client = OpenAI(
        api_key=require_env("GOOGLE_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )

    system_prompt = RESULT_REASON_SYSTEM_PROMPT_TEMPLATE.format(
        conversation_history=conversation_history,
        edition_result=edition_result,
    )

    try:
        response = client.chat.completions.create(
            model=DEFAULT_LLM,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Explain why this result appears."},
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
    except Exception:
        logger.exception(
            "get_result_reason: failed to generate explanation, using fallback"
        )
        return FALLBACK_RESULT_REASON, False


@result_reason_blueprint.route("/result-reason", methods=["POST"])
@require_api_key
@require_session_jwt
@timer(logger)
def result_reason(session_id):
    response_type = "result_reason"

    call_id = request.json.get("call_id")
    edition_id = request.json.get("edition_id")

    log_context = {"session_id": session_id}
    if call_id is not None:
        log_context["call_id"] = call_id
    if edition_id is not None:
        log_context["edition_id"] = edition_id

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

            if not edition_id:
                return APIUtils.formatResponseObject(
                    400, response_type, {"message": "edition_id is required"}
                )

            messages = get_session_messages(session_id)

            # --- 404 guard 1: session has no messages ---
            if not messages:
                logger.warning(
                    f"get_result_reason: no messages found for session '{session_id}'"
                )
                return APIUtils.formatResponseObject(
                    404,
                    response_type,
                    {"message": f"session '{session_id}' not found"},
                )

            function_call_args, function_call_output, function_call_idx = (
                get_tool_call_by_id(messages, call_id)
            )

            # --- 404 guard 2: tool call_id not in session ---
            if function_call_args is None or function_call_output is None:
                logger.warning(
                    f"get_result_reason: call_id '{call_id}' not found in session '{session_id}'"
                )
                return APIUtils.formatResponseObject(
                    404,
                    response_type,
                    {"message": f"call_id '{call_id}' not found in session"},
                )

            # --- 404 guard 3: tool call output is a tool error ---
            if function_call_output.startswith(TOOL_ERROR_PREFIX):
                logger.warning(
                    f"get_result_reason: tool output for call_id '{call_id}' is an error"
                )
                return APIUtils.formatResponseObject(
                    404,
                    response_type,
                    {"message": f"tool output for call_id '{call_id}' is an error"},
                )

            # --- 404 guard 4: edition_id not present in the tool call output ---
            edition_result = find_result_by_edition_id(function_call_output, edition_id)

            if edition_result is None:
                logger.warning(
                    f"get_result_reason: edition_id '{edition_id}' not found in tool output for call_id '{call_id}'"
                )
                return APIUtils.formatResponseObject(
                    404,
                    response_type,
                    {
                        "message": f"edition_id '{edition_id}' not found in results for call_id '{call_id}'"
                    },
                )

            # Truncate conversation history to end at (and including) this tool call's
            # arguments, excluding the tool call output.
            messages = messages[:function_call_idx]

            explanation, is_ai_generated = get_result_reason(messages, edition_result)
            logger.info(
                f"Result reason generated. Hardcoded fallback used?: {not is_ai_generated}"
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
