"""Shared persistent event loop for running async code from sync Flask request threads.

The openai-agents SDK shares a single httpx.AsyncClient across all calls for
connection pool reuse (see agents/models/openai_provider.py's shared_http_client()),
and that client is bound to whichever event loop first
creates it. Waitress runs each request in its own thread, so calling
asyncio.run() per-request would give the second concurrent request a fresh
loop that doesn't match the one the shared client is bound to, deadlocking
silently. Instead, any blueprint that runs coroutines that depend on
the openai-agents SDK's shared httpx.AsyncClient or other resources tied to a
specific event loop should do so via run_coroutine() below, which always runs
them on the same persistent background loop for the lifetime of the process.
"""

import asyncio
import threading

_loop = None
_loop_lock = threading.Lock()


def get_event_loop():
    """Return the shared background event loop, creating it (and its daemon
    thread) on first call. Safe to call from multiple threads."""
    global _loop
    if _loop is None:
        # threading.Lock() makes this threadsafe to be called concurrently in \
        # multiple threads and create only one event loop + thread.
        with _loop_lock:
            if _loop is None:
                loop = asyncio.new_event_loop()
                threading.Thread(
                    target=loop.run_forever, name="shared-event-loop", daemon=True
                ).start()
                _loop = loop
    return _loop


def run_coroutine(coro):
    """Run `coro` on the shared background event loop, blocking the calling
    thread until it completes, same as asyncio.run() would."""
    return asyncio.run_coroutine_threadsafe(coro, get_event_loop()).result()
