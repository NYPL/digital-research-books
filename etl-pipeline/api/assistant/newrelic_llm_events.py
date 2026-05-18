"""
New Relic custom LLM event instrumentation for the AI chat assistant.

Records LlmChatCompletionMessage and LlmChatCompletionSummary events
compatible with the New Relic AI monitoring dashboard.
"""

import json
import os
import time
import uuid
from typing import Optional

import newrelic.agent
from agents import Agent
from agents.items import ModelResponse
from agents.models.chatcmpl_converter import Converter

from logger import create_log

logger = create_log(__name__)


def record_llm_events(
    agent: Agent,
    *,
    session_id: str,
    conversation_type: str,
    edition_id: Optional[int],
    last_response: Optional[ModelResponse],
    last_system_prompt: Optional[str],
    last_input_items: list,
    total_input_tokens: int,
    total_output_tokens: int,
    total_llm_elapsed: float,
) -> None:
    """
    Custom instrumentation for New Relic AI monitoring.

    Logs a series of New Relic LLM events during an agent run, including:
    - Message events to describe the content, role, sequence, etc. of each turn:
        - System prompt
        - User prompt
        - Assistant decisions and tool calls
        - Assistant response seen by user
    - Summary event to report key metrics like duration and token usage.

    For dashboard compatibility, the vendor is set to "openai" for all events.

    No-ops when NEW_RELIC_CUSTOM_LLM_EVENT_LOGGING_ENABLED != "true".
    """
    if os.getenv("NEW_RELIC_CUSTOM_LLM_EVENT_LOGGING_ENABLED") != "true":
        return
    if not last_response:
        return

    # Extract text content from LLM response
    content = ""
    try:
        if hasattr(last_response, "output") and last_response.output:
            first_out = last_response.output[0]
            if hasattr(first_out, "content"):
                content_val = first_out.content
                if isinstance(content_val, list) and len(content_val) > 0:
                    content = getattr(content_val[0], "text", "")
                else:
                    content = str(content_val)
    except Exception as e:
        logger.warning(f"Failed to extract LLM message content: {e}")

    event_id_unique = str(uuid.uuid4())
    resp_id = str(uuid.uuid4())

    transaction = newrelic.agent.current_transaction()
    if transaction:
        transaction.add_ml_model_info("OpenAI", "1.0.0")
        transaction._add_agent_attribute("llm", True)

    trace_id = newrelic.agent.current_trace_id()
    span_id = newrelic.agent.current_span_id()

    timestamp_ms = int(time.time() * 1000)

    total_tokens = total_input_tokens + total_output_tokens

    sequence = 0

    # 1. Log event for system prompt
    if last_system_prompt:
        system_msg_payload = {
            "id": f"{event_id_unique}-{sequence}",
            "request_id": resp_id,
            "completion_id": event_id_unique,
            "llm.conversation_id": session_id,
            "content": last_system_prompt,
            "role": "system",
            "sequence": sequence,
            "response.model": agent.model.model,
            "vendor": "openai",
            "ingest_source": "Python",
            "trace_id": trace_id,
            "span_id": span_id,
            "timestamp": timestamp_ms,
        }
        newrelic.agent.record_custom_event(
            "LlmChatCompletionMessage", system_msg_payload
        )
        sequence += 1

    # 2. Log events for user, assistant, and tool calls in the order they're sent
    try:
        converted_msgs = Converter.items_to_messages(
            last_input_items, model=agent.model.model
        )
        for msg in converted_msgs:
            role = msg.get("role", "unknown")
            if role == "system":
                continue  # System prompt handled above

            msg_payload = {
                "id": f"{event_id_unique}-{sequence}",
                "request_id": resp_id,
                "completion_id": event_id_unique,
                "llm.conversation_id": session_id,
                "role": role,
                "sequence": sequence,
                "response.model": agent.model.model,
                "vendor": "openai",
                "ingest_source": "Python",
                "trace_id": trace_id,
                "span_id": span_id,
                "timestamp": timestamp_ms,
            }

            content_val = msg.get("content")
            tool_calls = msg.get("tool_calls")

            # On tool calls, clarify assistant role by appending the tool name.
            # Otherwise the NR dashboard will show the tool call in Responses table
            # because the table looks for the first assistant message with content.
            if not content_val and tool_calls:
                content_val = json.dumps(tool_calls)

                try:
                    tool_name = (
                        tool_calls[0].get("function", {}).get("name", "tool call")
                    )
                    msg_payload["role"] += f" ({tool_name})"
                except (IndexError, AttributeError, TypeError):
                    msg_payload["role"] += " (tool call)"

            if content_val:
                if (
                    role != "user"
                    or os.getenv("NEW_RELIC_CUSTOM_LLM_SEND_USER_PROMPT_TEXT_ENABLED")
                    == "true"
                ):
                    msg_payload["content"] = str(content_val)

            newrelic.agent.record_custom_event("LlmChatCompletionMessage", msg_payload)
            sequence += 1
    except Exception as e:
        logger.warning(f"Error logging previous messages: {e}")

    # 3. Log event for final response (i.e. the message the user sees)
    final_msg_payload = {
        "id": f"{event_id_unique}-{sequence}",
        "request_id": resp_id,
        "completion_id": event_id_unique,
        "llm.conversation_id": session_id,
        "content": content,
        "role": "assistant",
        "is_response": True,
        "sequence": sequence,
        "token_count": total_output_tokens,
        "response.model": agent.model.model,
        "vendor": "openai",
        "ingest_source": "Python",
        "trace_id": trace_id,
        "span_id": span_id,
        "timestamp": timestamp_ms,
    }
    newrelic.agent.record_custom_event("LlmChatCompletionMessage", final_msg_payload)

    # 4. Log summary event with aggregate LLM response metadata
    summary_payload = {
        "id": event_id_unique,
        "span_id": span_id,
        "trace_id": trace_id,
        "request.model": agent.model.model,
        "request.temperature": 0.0,
        "request.max_tokens": None,
        "vendor": "openai",
        "ingest_source": "Python",
        "request_id": resp_id,
        "duration": float(total_llm_elapsed * 1000),
        "response.model": agent.model.model,
        "response.organization": "",
        "response.choices.finish_reason": "stop",
        "response.number_of_messages": sequence + 1,
        "timestamp": timestamp_ms,
        "llm.conversation_id": session_id,
        "llm.conversation_type": conversation_type,
        "llm.edition_id": edition_id,
        "llm.is_final_turn": True,
        "response.usage.prompt_tokens": total_input_tokens,
        "response.usage.completion_tokens": total_output_tokens,
        "response.usage.total_tokens": total_tokens,
    }

    newrelic.agent.record_custom_event("LlmChatCompletionSummary", summary_payload)
