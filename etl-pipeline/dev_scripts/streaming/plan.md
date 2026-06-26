# Streaming Backend Refactor Plan

## Goal

Convert the `/chat` endpoint to stream LLM responses via Server-Sent Events (SSE), so the
frontend can progressively render the assistant's text as it's generated and show live search
progress rather than waiting for the full response before displaying anything.

The existing Flask app is preserved unchanged. A new FastAPI app handles `/vra/chat`.
Both are served from a single Hypercorn process via a combined ASGI entry point.

---

## Context for the Implementer

### What the current system does

`POST /chat` (Flask, `api/blueprints/chat.py`) accepts a user message and runs an OpenAI
Agents SDK agent loop via `update_chat()` (`api/assistant/agent.py`). The agent uses one of
two search tools (`search_catalog` or `search_book`) to query a Turbopuffer vector DB, then
generates a text response. The full result is returned as NDJSON once the entire agent run
completes — the user sees nothing until the run is done.

The existing streaming is done via a thread + `queue.Queue` bridge: `asyncio.run(update_chat())`
runs in a daemon thread; the main thread yields NDJSON lines from the queue. This works but
only emits coarse events (`search_started`, `search_completed`) — there is
no token-level streaming.

### Why a separate FastAPI app instead of upgrading Flask

Flask is a WSGI framework. WSGI is synchronous and cannot natively iterate an async generator —
`Runner.run_streamed().stream_events()` from the OpenAI Agents SDK is an async iterator.
Converting Flask to an async-capable framework (Quart) would require touching every blueprint,
decorator, and extension — out of scope. Instead, the single streaming endpoint is extracted
into a lightweight FastAPI app (ASGI-native) and the Flask app is mounted inside it as a WSGI
sub-app via `starlette.middleware.wsgi.WSGIMiddleware`. Flask routes are completely unchanged.

### The `search_results` data structure

`search_results` is a `dict` on the execution context, keyed by `tool_call_id` (the ID the
LLM assigned to the tool call). It is populated inside the search tool functions
(`search_catalog`, `search_book`) during the agent run. Each entry:

```python
search_results[tool_call_id] = {
    "tool_name": "search_catalog" | "search_book",
    "edition_data": [CatalogSearchResult | ContentSearchResult, ...],
    "search_params": {...},   # the raw tool call arguments
}
```

`CatalogSearchResult` / `ContentSearchResult` are dataclasses in `api/assistant/types.py`.
They hold ORM objects and chunk hits. After `get_relevant_snippets()` runs, each
`edition_result.snippets` list is populated with `Snippet` dataclass instances.

### Why snippets are deferred and only on the last result

`get_relevant_snippets` (`api/assistant/snippets.py`) runs after the full agent turn completes
because it needs the complete `RunResultStreaming` object (all turns finished). The naive
implementation only processes the last search result entry by design — if the agent called the
search tool multiple times, only the final call's results get snippets. This is intentional.
`_collect_final_snippets` in `chat_app.py` mirrors that scope and must not be changed to
collect from all results.

### Why each SSE event exists

| Event | Frontend purpose |
|---|---|
| `message_start` | Create a new (empty) streaming text bubble when the LLM begins generating a message |
| `message_delta` | Append token to the streaming text bubble as the LLM generates it |
| `message_end` | Finalize the bubble with the complete text once generation is done (can replace streamed content to guarantee correctness) |
| `search_start` | Show a "Searching…" indicator immediately when the LLM commits to a query (before the search executes) |
| `search_end` | Populate the results panel with formatted edition/snippet-free data as soon as the search completes |
| `search_error` | Show an error state for a search that produced no results entry (defensive — should not occur normally) |
| `final_search_snippets` | Augment the already-displayed results with snippet text once the post-run snippet computation finishes |
| `error` | Surface unhandled exceptions, LLM-level errors (`response.failed`, `response.incomplete`), and LLM refusals (`response.refusal.done` — warning-logged server-side, returned as `code: "content_refused"`) to the client |
 <!-- TODO: add a message_id_map event at the end of a successful agent turn. this will map agent message items to their db ids from Session.inserted_items -->

 <!-- TODO: make sure the max_turns_error handling case sends the garceful response correctly (as message_ events) -->

### Key files to read before implementing

```
api/blueprints/chat.py           current Flask view + prepare_search_response (being replaced)
api/assistant/agent.py           update_chat(), search_catalog(), search_book(), execution contexts
api/assistant/types.py           CatalogSearchResult, ContentSearchResult, Snippet dataclasses
api/assistant/snippets.py        get_relevant_snippets(), get_relevant_snippets_naive()
api/assistant/streaming_utils.py existing NDJSON serialize_event() / format_error()
api/decorators.py                require_api_key, require_session_jwt — verify_session/sign_session live here
api/app.py                       API class, current waitress serve() call, blueprint registration
```

---

## Architecture Overview

```
HTTP request
     │
     ▼
FastAPI (ASGI top-level)
  ├── CORSMiddleware (all routes)
  ├── POST /vra/chat  ──────────────→ FastAPI route → StreamingResponse (SSE)
  └── mount("/") → WSGIMiddleware
                       └── Flask app (all existing routes, unchanged)
     │
     ▼
uvicorn.run(combined_app, ...)
```

**Why FastAPI as top-level (not Starlette router, not custom dispatcher):**
- FastAPI ships with Starlette, so `starlette.middleware.wsgi.WSGIMiddleware` is available with no extra packages
- Defining the route at `/vra/chat` directly on the FastAPI app avoids path prefix stripping — FastAPI sees the full path, no surprising behavior
- FastAPI generates OpenAPI docs for the new endpoint automatically
- Single clean entry point for Hypercorn

---

## CORS Explanation

The current config is `CORS(self.app)` with no arguments. flask-cors defaults to:
- `Access-Control-Allow-Origin: *` — any website can call the API
- All methods and headers allowed
- **Credentials NOT included** — wildcard origins and `credentials: include` cannot coexist per the browser spec, so session cookies are not sent cross-origin. This is likely intentional: the frontend and API share an origin in production.

**Plan:** Remove `flask-cors`, add `CORSMiddleware` to the combined FastAPI app with identical
wildcard settings. This avoids duplicate CORS headers (which would occur if flask-cors AND
FastAPI's middleware both ran on Flask routes). One less dependency.

---

## New Packages Required

Add to `requirements.txt`:
```
fastapi>=0.115.0
uvicorn[standard]>=0.30.0
```

**Remove:** `waitress` (replaced by Uvicorn), `flask-cors` (replaced by `CORSMiddleware`).

`starlette` and `anyio` are pulled in transitively by `fastapi` — no explicit pin needed.

`starlette` and `anyio` are pulled in transitively by `fastapi` — no explicit pin needed.

---

## Dev Workflow

Uvicorn supports inline `uvicorn.run(app_object, ...)` for both dev and prod, matching the
existing `API.run()` pattern in `api/app.py`. No CLI entrypoint or separate `asgi.py` module
is required.

**Dev:** `uvicorn.run(self.combined_app, host=..., port=5050, log_level="debug")` — mirrors
`self.app.run(..., debug=True)` today.

**Prod:** `uvicorn.run(self.combined_app, host="0.0.0.0", port=80)` — mirrors
`serve(self.app, ...)` (waitress) today.

Note: passing an app *object* to `uvicorn.run()` disables hot reload (`reload=True` requires a
string import path). This matches current behaviour — the existing Flask dev server also runs
without the file-watcher reloader when invoked via `API.run()`.

---

## Files to Create

```
api/chat_app.py              FastAPI router: route handler, SSE stream generator
api/auth_dependencies.py     FastAPI Depends() equivalents for require_api_key / require_session_jwt
```

## Files to Modify

```
requirements.txt                   add fastapi, uvicorn[standard]; remove waitress, flask-cors
api/app.py                         build combined ASGI app; switch serve() to uvicorn
api/blueprints/chat.py             replace prepare_search_response with format_search_result;
                                   remove Flask view + streaming infrastructure; add ORM constants
api/assistant/agent.py             Runner.run → run_streamed; remove event_callback
api/assistant/streaming_utils.py   add SSE formatters + sse_error_response utility
```

---

## DRY / Generic Design Decisions

These apply across multiple files and are called out here to prevent duplication during implementation.

### D1. ORM exclude lists as module-level constants (`api/blueprints/chat.py`)

Currently hardcoded inline in `prepare_search_response`. Extract before implementing `format_search_result`:

```python
# api/blueprints/chat.py (module level)
from .models import Edition, Item, Link, Rights, Work  # adjust import path

_EDITION_ORM_EXCLUDE = [
    (Edition, "date_created"), (Edition, "date_modified"), (Edition, "dcdw_uuids"),
    (Item, "date_created"), (Item, "date_modified"), (Item, "modified"),
    (Item, "publisher_project_source"), (Item, "record_id"),
    (Link, "date_created"), (Link, "date_modified"), (Link, "md5"),
    (Rights, "date_created"), (Rights, "date_modified"), (Rights, "id"),
    (Rights, "rights_date"), (Rights, "rights_reason"),
]
_WORK_ORM_EXCLUDE = [(Work, "date_created"), (Work, "date_modified")]
_EDITION_COLUMN_FORMATTERS = {"publication_date": lambda d: d.year if d else None}
```

### D2. Tool-name → result-type mapping as a constant (`api/blueprints/chat.py`)

Replace the chained ternary that appears in both `prepare_search_response` (old) and
`format_search_result` (new) with a dict:

```python
TOOL_NAME_TO_RESULT_TYPE: dict[str, str] = {
    "search_catalog": "catalogSearch",
    "search_book": "contentSearch",
}
```

Used by `format_search_result` and by `_handle_stream_event` in `chat_app.py` (import it).

### D3. `format_search_result` replaces `prepare_search_response` (`api/blueprints/chat.py`)

A single function that formats one search result entry. Used by both `search_end` SSE events
(no snippets) and any future path that needs the formatted result. `prepare_search_response`
(which took the full dict and extracted the last entry) is deleted — it was only used in the
old streaming path being removed.

```python
def format_search_result(search_result: dict) -> tuple[str, dict]:
    """
    Format a single search_results entry into (result_type, api_dict).
    Does NOT include snippets — snippets are added via final_search_snippets.
    """
    result_type = TOOL_NAME_TO_RESULT_TYPE.get(search_result["tool_name"])
    if result_type is None:
        raise ValueError(f"Unknown tool: {search_result['tool_name']}")

    search_params = search_result["search_params"]
    editions = []

    for edition_result in search_result["edition_data"]:
        edition = {}  # no snippets key

        if result_type == "catalogSearch":
            edition_metadata = orm_to_dict(
                edition_result.orm_edition,
                exclude=_EDITION_ORM_EXCLUDE,
                column_formatters=_EDITION_COLUMN_FORMATTERS,
            )
            work_metadata = {
                f"work_{k}": v
                for k, v in orm_to_dict(
                    edition_result.orm_work, exclude=_WORK_ORM_EXCLUDE
                ).items()
            }
            edition.update({**edition_metadata, **work_metadata})

        editions.append(edition)

    if result_type == "catalogSearch":
        result = {
            "editions": editions,
            "search_params": search_params,
            "paging": APIUtils.formatPagingOptions(
                page=1, pageSize=PAGE_SIZE, totalHits=len(editions)
            ),
        }
    else:  # contentSearch
        result = {"search_params": search_params}

    return result_type, result
```

### D4. Auth type aliases (`api/auth_dependencies.py`)

Declare `SessionIdDep` and `ApiKeyDep` as separate type aliases and list both explicitly on
each route:

```python
ApiKeyDep = Annotated[None, Depends(get_api_key)]
SessionIdDep = Annotated[str, Depends(get_session_id)]
```

```python
@chat_router.post("/vra/chat")
async def chat(session_id: SessionIdDep, _api_key: ApiKeyDep, body: ChatRequest):
```

### D5. `sse_error_response` as a utility in `streaming_utils.py`

Any future FastAPI SSE endpoint needs this same helper. Keep it in the shared utils module,
not inline in `chat_app.py`:

```python
# api/assistant/streaming_utils.py
from fastapi.responses import StreamingResponse

def sse_error_response(message: str, code: str | None = None) -> StreamingResponse:
    """Return a one-shot SSE response with a single error event. For use in FastAPI routes."""
    async def gen():
        yield format_sse_error(message, code)
    return StreamingResponse(gen(), media_type="text/event-stream")
```

### D6. Named stream generator (`api/chat_app.py`)

Extract the `generate()` closure into a named module-level async generator. Makes it
independently testable without invoking the full FastAPI route.

```python
async def stream_chat_response(
    streaming_result: RunResultStreaming,
    search_results: dict,
    session_id: str,
    conversation_type: str,
    edition_id: int | None,
) -> AsyncGenerator[str, None]:
    try:
        async for event in streaming_result.stream_events():
            line = _handle_stream_event(event, search_results)
            if line:
                yield line

        await get_relevant_snippets(streaming_result, approach="naive")
        yield format_sse("final_search_snippets", _collect_final_snippets(search_results))

        record_llm_events(
            streaming_result,
            session_id=session_id,
            conversation_type=conversation_type,
            edition_id=edition_id if conversation_type == "contentSearch" else None,
        )

    except Exception as e:
        logger.exception("Error in streaming chat handler")
        yield format_sse_error(str(e), code="stream_error")
```

---

## 1. `requirements.txt`

```diff
-waitress==3.0.1
-flask-cors==6.0.0
+fastapi>=0.115.0
+uvicorn[standard]>=0.30.0
```

---

## 2. `api/app.py` — Build combined ASGI app

```python
# New imports (replace waitress + flask-cors)
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.wsgi import WSGIMiddleware

from .chat_app import chat_router


class API:
    def __init__(self, redis_client):
        # --- existing Flask app setup (unchanged) ---
        Session = get_readonly_session()
        self.app = Flask(__name__)
        # REMOVE: CORS(self.app) — handled at ASGI layer below

        Swagger(self.app, ...)
        self.app.config["SQL_ENGINE"] = get_readonly_engine()
        self.app.config["REDIS_CLIENT"] = redis_client
        self.app.config["READER_VERSION"] = os.environ["READER_VERSION"]
        self._register_blueprints()

        @self.app.teardown_appcontext
        def shutdown_db_session(exception=None):
            Session.remove()

        # --- combined ASGI app ---
        combined = FastAPI(
            docs_url="/vra/docs",
            redoc_url="/vra/redoc",
            openapi_url="/vra/openapi.json",
        )
        combined.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )
        combined.include_router(chat_router)
        combined.mount("/", WSGIMiddleware(self.app))  # Flask catch-all

        self.combined_app = combined

    def _register_blueprints(self):
        for blueprint in BLUEPRINTS:
            self.app.register_blueprint(blueprint)

    def run(self):
        logger.info(f"API server running in process id: {os.getpid()}")
        if os.environ.get("STAGE") == "development":
            uvicorn.run(
                self.combined_app,
                host=os.environ.get("DRB_API_HOST", "localhost"),
                port=5050,
                log_level="debug",
            )
        else:
            uvicorn.run(self.combined_app, host="0.0.0.0", port=80)

    def create_error_responses(self):
        # unchanged — Flask error handlers still apply to Flask routes
        ...
```

**Remove** `from waitress import serve` and `from flask_cors import CORS`.

---

## 3. `api/auth_dependencies.py` — FastAPI auth dependencies

Reuses the existing pure-Python `verify_session`, `sign_session`, `verify_api_key` functions
unchanged. Only the request/response wiring is new.

```python
import os
from typing import Annotated
from uuid import uuid4

from fastapi import Cookie, Depends, Header, HTTPException, Response

# Adjust these imports to wherever the functions actually live in api/decorators.py
from .decorators import verify_session, sign_session, verify_api_key


async def get_api_key(x_api_key: str | None = Header(None)) -> None:
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    try:
        verify_api_key(x_api_key)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid API key")


async def get_session_id(
    response: Response,
    vra_session: str | None = Cookie(None),
) -> str:
    is_dev = os.environ.get("ENVIRONMENT") == "local"
    cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")

    if vra_session:
        try:
            return verify_session(vra_session)
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid session")

    new_uuid = str(uuid4())
    try:
        token = sign_session(new_uuid)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to create session")

    response.set_cookie(
        cookie_name, token, httponly=True, secure=not is_dev, samesite="lax", path="/"
    )
    return new_uuid


# Composed dep — see D4
async def require_chat_auth(
    session_id: Annotated[str, Depends(get_session_id)],
    _: Annotated[None, Depends(get_api_key)],
) -> str:
    return session_id

ChatAuthDep = Annotated[str, Depends(require_chat_auth)]
```

---

## 4. `api/assistant/streaming_utils.py` — SSE formatters

Add alongside existing NDJSON helpers (keep old ones until tests are updated):

```python
import json
from typing import Any

from fastapi.responses import StreamingResponse


def format_sse(event_type: str, data: dict[str, Any]) -> str:
    serialized = json.dumps(data, separators=(",", ":"), default=_default_json_serializer)
    return f"event: {event_type}\ndata: {serialized}\n\n"


def format_sse_error(message: str, code: str | None = None) -> str:
    data = {"message": message}
    if code:
        data["code"] = code
    return format_sse("error", data)


def sse_error_response(message: str, code: str | None = None) -> StreamingResponse:
    """One-shot SSE response with a single error event. Reusable across FastAPI routes."""
    async def gen():
        yield format_sse_error(message, code)
    return StreamingResponse(gen(), media_type="text/event-stream")
```

`_default_json_serializer` is unchanged — reused by both NDJSON and SSE formatters.

---

## 5. `api/blueprints/chat.py` — Extract constants + replace formatting function

### 5a. Add module-level constants (see D1, D2)

Add `_EDITION_ORM_EXCLUDE`, `_WORK_ORM_EXCLUDE`, `_EDITION_COLUMN_FORMATTERS`,
and `TOOL_NAME_TO_RESULT_TYPE` at the top of the file (after imports).

### 5b. Replace `prepare_search_response` with `format_search_result` (see D3)

Delete `prepare_search_response` entirely (the old streaming path that called it is removed).
Add `format_search_result(search_result: dict) -> tuple[str, dict]` using the new constants.

### 5c. Remove Flask view and streaming infrastructure

Delete:
- `@chat_blueprint.route("", methods=["POST"])` view and inner functions
- `_chat_handler`, `_chat_stream_handler`, `generate_streaming_response`
- `on_event`, `event_queue`, threading boilerplate (`import threading`, `import queue`)
- `format_final_response` import from `streaming_utils` (no longer emitted)

If `chat_blueprint` has no remaining routes, remove it from `BLUEPRINTS` in `app.py` and
delete the file. Confirm nothing else imports from it first.

---

## 6. `api/assistant/agent.py` — Switch to `run_streamed`, remove event_callback

### 6a. Remove `event_callback` from execution contexts

Delete the `event_callback: Optional[EventCallback] = None` field from both
`CatalogSearchExecutionContext` and `ContentSearchExecutionContext` (~lines 257, 271).

Also delete `EventCallback` type alias and `emit_stream_event()` function if confirmed
unused outside agent.py:
```bash
grep -rn "emit_stream_event\|EventCallback" api/
```

### 6b. Remove `emit_stream_event` calls from search tools

Delete all four calls in `search_catalog` (~lines 752, 885) and `search_book`
(~lines 921, 981). These `search_started` / `search_completed` events are replaced by
`search_start` / `search_end` SSE events derived from `stream_events()`.

### 6c. Switch `update_chat` to return `RunResultStreaming`

```python
from agents import Runner, RunResultStreaming

async def update_chat(...) -> RunResultStreaming:  # remove event_callback param
    ...
    # Remove event_callback= from exec_context construction

    # Runner.run_streamed() is synchronous — do NOT await it
    streaming_result = Runner.run_streamed(
        starting_agent=agent,
        input=message,
        context=exec_context,
        hooks=logging_hooks,
        max_turns=max_turns,
        error_handlers={"max_turns": _on_max_turns},
        session=session,
        run_config=RunConfig(...),
    )
    # record_llm_events moved to stream_chat_response() — raw_responses not complete yet
    return streaming_result
```

**Sync DB calls in async context:** Wrap `get_frbr_data_by_edition` (and any other sync DB
calls inside `update_chat`) with `asyncio.to_thread`:
```python
frbr_data = await asyncio.to_thread(get_frbr_data_by_edition, [edition_id])
```

**Verify:** Confirm `error_handlers`, `hooks`, `session`, `run_config` are accepted by
`Runner.run_streamed` (should be identical kwargs to `Runner.run` — check agents SDK source).

---

## 7. `api/chat_app.py` — New FastAPI router

### 7a. New SSE Event Protocol

All events: `event: <type>\ndata: <json>\n\n`

| Event | Trigger | Payload |
|---|---|---|
| `message_start` | `RawResponsesStreamEvent`: `response.output_item.added` where `item.type == "message"` | `{ "item_id": "..." }` |
| `message_delta` | `RawResponsesStreamEvent`: `response.output_text.delta` | `{ "delta": "..." }` |
| `message_end` | `RawResponsesStreamEvent`: `response.output_text.done` | `{ "text": "..." }` |
| `search_start` | `RunItemStreamEvent`: `tool_called` (args fully generated, before execution) | `{ "tool_name": "...", "args": {...} }` |
| `search_end` | `RunItemStreamEvent`: `tool_output` + `search_results[tool_call_id]` exists | `{ "tool_call_id": "...", "result_type": "...", "result": {...} }` |
| `search_error` | `RunItemStreamEvent`: `tool_output` + no `search_results` entry | `{ "tool_call_id": "..." }` |
| `final_search_snippets` | After stream exhausted, snippets computed for final result | `{ "<edition_id>": [Snippet, ...] }` |
| `error` | Unhandled exception in stream generator; `ResponseErrorEvent` / `response.incomplete` / `response.failed` from LLM (`code: "llm_error"`); LLM refusal (`code: "content_refused"`, full refusal text warning-logged server-side) | `{ "message": "...", "code": "..." }` |

### 7b. `ChatRequest` Pydantic model

Move enum validation and type coercion out of the route handler:

```python
from pydantic import BaseModel
from typing import Literal

class ChatRequest(BaseModel):
    conversationType: Literal["contentSearch", "catalogSearch"]
    message: str
    editionId: int | None = None
```

FastAPI returns HTTP 422 for Pydantic validation failures. The `editionId` required-for-contentSearch
check cannot be done purely in Pydantic without customizing the 422 response to SSE format, so
keep that one check in the route body and return `sse_error_response(...)`.

### 7c. `_handle_stream_event`

```python
import json
import logging
from agents.stream_events import RawResponsesStreamEvent, RunItemStreamEvent
from .blueprints.chat import TOOL_NAME_TO_RESULT_TYPE, format_search_result  # D2, D3

logger = logging.getLogger(__name__)

_LLM_ERROR_TYPES = {"error", "response.incomplete", "response.failed"}

def _handle_stream_event(event, search_results: dict) -> str | None:
    if isinstance(event, RawResponsesStreamEvent):
        event_type = getattr(event.data, "type", None)

        if event_type == "response.output_item.added":
            # Only emit message_start for text message items, not function calls
            if getattr(event.data.item, "type", None) == "message":
                return format_sse("message_start", {"item_id": event.data.item.id})
            return None

        if event_type == "response.output_text.delta":
            return format_sse("message_delta", {"delta": event.data.delta})

        if event_type == "response.output_text.done":
            return format_sse("message_end", {"text": event.data.text})

        # response.refusal.delta: ignore individual deltas; full text is on .done
        if event_type == "response.refusal.delta":
            return None

        if event_type == "response.refusal.done":
            logger.warning("LLM refused request: %s", event.data.refusal)
            return format_sse_error(
                "The assistant refused to respond to this request.",
                code="content_refused",
            )

        if event_type in _LLM_ERROR_TYPES:
            msg = getattr(event.data, "message", None) or event_type
            return format_sse_error(msg, code="llm_error")

        # Unhandled raw response events (not needed for this application):
        # response.created, response.in_progress, response.queued — lifecycle events, no frontend action
        # response.content_part.added / .done — content part boundaries; message_start/end covers this
        # response.output_item.done — item completion boundary; message_end (output_text.done) covers text
        # response.function_call_arguments.delta / .done — tool call argument streaming; search_start covers commit point
        # response.audio.delta / .done, response.audio_transcript.* — audio output, not used
        # response.reasoning.delta / .done, response.reasoning_summary.* — reasoning model output, not used
        # response.web_search_call.*, response.file_search_call.*, response.code_interpreter_call.* — built-in tools, not used
        # response.image_gen_call.*, response.mcp_call.*, response.custom_tool_call.* — not used
        # response.output_text.annotation.added — citation annotations, not used
        return None

    if isinstance(event, RunItemStreamEvent):
        if event.name == "tool_called":
            return format_sse("search_start", {
                "tool_name": event.item.tool_name,
                "args": json.loads(event.item.arguments),
            })
        if event.name == "tool_output":
            tool_call_id = event.item.tool_call_id
            if tool_call_id in search_results:
                result_type, formatted = format_search_result(search_results[tool_call_id])
                return format_sse("search_end", {
                    "tool_call_id": tool_call_id,
                    "result_type": result_type,
                    "result": formatted,
                })
            return format_sse("search_error", {"tool_call_id": tool_call_id})

        # Unhandled RunItemStreamEvent names (not needed for this application):
        # message_output_created — high-level message item created; message_start (output_item.added) covers this
        # handoff_requested / handoff_occured — agent handoffs; single-agent setup, will not fire
        # tool_search_called / tool_search_output_created — built-in file/web search tools, not used
        # reasoning_item_created — reasoning model items, not used
        # mcp_approval_requested / mcp_approval_response / mcp_list_tools — MCP approval flow, not used
        return None

    # AgentUpdatedStreamEvent: fires when a new agent takes over during a handoff.
    # Single-agent setup — will not fire.
    return None
```

### 7d. `_collect_final_snippets`

Mirrors `get_relevant_snippets_naive`: only the **last** search result entry gets snippets.
Do NOT change `snippets.py` — `get_relevant_snippets_naive` already only processes the last
result, and `_collect_final_snippets` matches that scope intentionally.

```python
from dataclasses import asdict

def _collect_final_snippets(search_results: dict) -> dict:
    if not search_results:
        return {}
    _, last_result = list(search_results.items())[-1]
    return {
        str(edition_result.edition_id): [
            asdict(s)
            for s in sorted(
                edition_result.snippets,
                key=lambda s: s.chunk_score,
                **SCORE_SORT_DIRECTION,
            )
        ]
        for edition_result in last_result["edition_data"]
    }
```

### 7e. `stream_chat_response` — named async generator (see D6)

```python
from collections.abc import AsyncGenerator

async def stream_chat_response(
    streaming_result: RunResultStreaming,
    search_results: dict,
    session_id: str,
    conversation_type: str,
    edition_id: int | None,
) -> AsyncGenerator[str, None]:
    try:
        async for event in streaming_result.stream_events():
            line = _handle_stream_event(event, search_results)
            if line:
                yield line

        await get_relevant_snippets(streaming_result, approach="naive")
        yield format_sse("final_search_snippets", _collect_final_snippets(search_results))

        record_llm_events(
            streaming_result,
            session_id=session_id,
            conversation_type=conversation_type,
            edition_id=edition_id if conversation_type == "contentSearch" else None,
        )

    except Exception as e:
        logger.exception("Error in streaming chat handler")
        yield format_sse_error(str(e), code="stream_error")
```

### 7f. Route handler

```python
import newrelic.agent
from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from .auth_dependencies import ApiKeyDep, SessionIdDep
from .assistant.agent import update_chat, record_llm_events
from .assistant.snippets import get_relevant_snippets
from .assistant.streaming_utils import format_sse, format_sse_error, sse_error_response

chat_router = APIRouter()


@chat_router.post("/vra/chat")
async def chat(session_id: SessionIdDep, _api_key: ApiKeyDep, body: ChatRequest):
    if conversation_type == "contentSearch" and body.editionId is None:
        return sse_error_response("editionId is required for contentSearch", "validation_error")

    newrelic.agent.add_custom_attribute("session_id", session_id)
    newrelic.agent.add_custom_attribute("conversation_type", body.conversationType)
    if body.editionId is not None:
        newrelic.agent.add_custom_attribute("edition_id", body.editionId)
    newrelic.agent.add_custom_attribute("llm.conversation_id", session_id)

    streaming_result = await update_chat(
        body.message, body.conversationType, session_id, edition_id=body.editionId
    )

    return StreamingResponse(
        stream_chat_response(
            streaming_result=streaming_result,
            search_results=streaming_result.context_wrapper.context.search_results,
            session_id=session_id,
            conversation_type=body.conversationType,
            edition_id=body.editionId,
        ),
        media_type="text/event-stream",
    )
```

---

## 8. Verify: SDK stream event field names

Before coding `_handle_stream_event`, confirm exact attribute paths by reading the source:

```bash
uv run python -c "import agents.stream_events; print(agents.stream_events.__file__)"
```

Check:
- `RawResponsesStreamEvent.data.type` — value for text token deltas (`"response.output_text.delta"`?)
- `RawResponsesStreamEvent.data.delta` — is it a `str` directly, or a nested object?
- `RunItemStreamEvent.item` for `tool_called` — fields: `tool_name`, `tool_call_id`, `arguments`
- `RunItemStreamEvent.item` for `tool_output` — field: `tool_call_id`

---

## Open Questions / Risks

| # | Issue | Action |
|---|---|---|
| 1 | `Runner.run_streamed` kwargs | Verify `error_handlers`, `session`, `hooks`, `run_config` — check agents SDK source |
| 2 | Sync DB calls in `update_chat` | Wrap `get_frbr_data_by_edition` (and others) with `asyncio.to_thread()` |
| 3 | `record_llm_events` post-stream | Confirm all fields it reads from `RunResult` exist on `RunResultStreaming` after stream exhaustion |
| 4 | `redis_client` at startup | Confirm how `redis_client` is obtained when `API` is instantiated; verify it is available before `uvicorn.run()` is called |
| 5 | New Relic ASGI | Verify `newrelic.agent.add_custom_attribute` works inside async FastAPI routes; NR supports ASGI but configuration may differ |
| 6 | `MaxTurnsExceeded` from `stream_events()` | Catch explicitly in `stream_chat_response` before the bare `except Exception` — yield a typed error event -- look into the API and docs for @api/assistant/agent.py:641-642  |
| 7 | `@timer` decorator on `update_chat` | Confirm it handles `async def` correctly; update if not |
| 8 | SDK field names | Verify before coding `_handle_stream_event` — see section 8 |
