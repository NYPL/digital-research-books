"""
Utilities for streaming progress events in NDJSON format.

Each event is serialized as a single JSON line that can be safely streamed
to the client without any newline confusion.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID


def serialize_event(event_type: str, data: Dict[str, Any]) -> str:
    """
    Serialize an event as a single NDJSON line.

    Args:
        event_type: The event type (e.g., "search_started", "final_response")
        data: Event-specific data dict

    Returns:
        A JSON string with the event, ending in a newline
    """
    event = {"type": event_type, **data}
    return json.dumps(event, separators=(",", ":")) + "\n"


def format_error(message: str, code: Optional[str] = None) -> str:
    """
    Format and serialize an error event.

    Args:
        message: Error message
        code: Optional error code

    Returns:
        NDJSON line for error event
    """
    data = {"message": message}
    if code:
        data["code"] = code

    return serialize_event("error", data)


def format_final_response(
    messages: list,
    result_type: Optional[str],
    result: Optional[Dict],
    session_id: str,
) -> str:
    """
    Format and serialize a final_response event.

    Args:
        messages: List of message objects
        result_type: Type of result ("catalogSearch", "contentSearch", or None)
        result: Result data object
        session_id: Session identifier

    Returns:
        NDJSON line for final_response event
    """
    return serialize_event(
        "final_response",
        {
            "messages": messages,
            "result_type": result_type,
            "result": result,
            "session_id": session_id,
        },
    )
