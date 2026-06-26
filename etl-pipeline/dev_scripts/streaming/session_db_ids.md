# DB Message IDs in the Streaming Context

## Why this exists

The OpenAI Agents SDK stamps `__fake_id__` into tool-call and tool-output items when
converting from the Chat Completions API (see `agents/fake_id.py`). `CustomSQLAlchemySession`
(`api/assistant/session.py`) overrides `add_items()` to use PostgreSQL's
`INSERT ... RETURNING id`, capturing the real `agent_messages.id` for every item as it is
persisted. After a run completes, `session.inserted_items` returns
`list[tuple[int, TResponseInputItem]]` — one entry per row written, in insertion order.

## Non-streaming (current)

```python
session = CustomSQLAlchemySession(session_id, engine=get_async_engine())
run_result = asyncio.run(update_chat(..., session=session))
# session.inserted_items is fully populated here
messages = [{"db_id": db_id, **item} for db_id, item in session.inserted_items]
```

`add_items()` is called once per agent turn by the SDK (user input + all new items in a
single batch). For a single-turn request this means one `add_items()` call; multi-turn tool
loops call it once per turn, accumulating into `_inserted_items`.

## Streaming (future — Runner.run_streamed)

`Runner.run_streamed()` returns `RunResultStreaming` immediately without running the agent.
The agent runs as `stream_events()` is consumed. Internally the SDK still calls
`session.add_items()` per turn during streaming, so `_inserted_items` accumulates in the
same way. The difference is timing: items arrive incrementally, not all at once.

```python
session = CustomSQLAlchemySession(session_id, engine=get_async_engine())
streaming_result = await update_chat(..., session=session)  # returns RunResultStreaming

async for event in streaming_result.stream_events():
    # agent_messages rows are being written here as turns complete
    # session.inserted_items grows with each add_items() call
    if event.type == "run_item_stream_event" and event.name == "message_output_created":
        # The item has just been generated but may not be persisted yet —
        # add_items() fires at the end of each turn, not per item.
        ...

# After stream_events() is exhausted, all turns are done and all rows are written.
messages = [
    {"db_id": db_id, **item}
    for db_id, item in session.inserted_items
    if item.get("role") != "user"
]
```

**Key constraint:** do not read `session.inserted_items` until `stream_events()` is fully
consumed. Items are persisted per-turn, so mid-stream the list may be incomplete.

**Confirmed persistence timing (from SDK source):**

1. **User input** — persisted in a single `add_items()` call *before* the model stream starts.
   - `agents/run_internal/run_loop.py::run_single_turn_streamed` lines 1139–1154: guards on
     `not streamed_result._stream_input_persisted`, then calls
     `save_result_to_session(session, input_items_to_save, [], ...)` directly.

2. **Response items** (tool calls, tool outputs, assistant messages) — persisted in a single
   `add_items()` call *after* `run_single_turn_streamed` returns with a complete
   `SingleStepResult`. Zero DB writes occur during token streaming. Call sites in
   `agents/run_internal/run_loop.py::start_streaming` by next-step type:
   - `NextStepRunAgain` → line 949: `_save_stream_items_with_count(turn_session_items, ...)`
   - `NextStepFinalOutput` → line 909: `_finalize_streamed_final_output(..., save_items=_save_stream_items_with_count, ...)` → `save_items(items, ...)` at line 347
   - `NextStepHandoff` → line 887: `_save_stream_items_without_count(turn_session_items, ...)`
   - `NextStepInterruption` → line 935: `_finalize_streamed_interruption(..., save_items=_save_stream_items_with_count, ...)` → `save_items(items, ...)` at line 362

   All four paths converge on `_save_stream_items` (lines 270–298) →
   `save_result_to_session` (`agents/run_internal/session_persistence.py` line 286) → `session.add_items()`.

This means `inserted_items` is only fully populated once `stream_events()` is exhausted.

## What inserted_items contains

`inserted_items` covers every item saved by `add_items()` during the session object's
lifetime — including the user's original input message. Items appear in insertion order:

```
[(101, {"role": "user",      "content": "...", ...}),   # user message
 (102, {"type": "function_call", "id": "__fake_id__", ...}),  # tool call
 (103, {"type": "function_call_output", ...}),          # tool output
 (104, {"role": "assistant", "content": "...", ...})]   # final response
```

The `db_id` replaces the `__fake_id__` as the stable identifier for each message.
`role`/`type` fields distinguish message kinds if the frontend needs to filter.



TODO: return a message id mapping event at the end of the turn end... message order->db id
- Use get_new_items_with_ids(), create unit tests that guarentee that get_new_items_with_ids() is called after runResults.is_complete==True (so we now)
- we will start by returning mapping for all new_items but really a mapping only for agent message items is necessary (or even helpful)








