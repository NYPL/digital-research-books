"""
Interactive debug helper for comparing snippet selection results.

Usage (in a REPL or scratch script after running update_chat + get_relevant_snippets):

    from scratch.relevant_snippets.debug_snippets import display_snippet_comparison

    run_result = asyncio.run(update_chat(conversation, conversation_type, edition_id=edition_id))
    await get_relevant_snippets(run_result)
    display_snippet_comparison(run_result)

The naive snippet section uses _apply_naive_snippets (the same logic as
get_relevant_snippets_naive) applied to a temporary copy of each entry, so the
LLM-selected snippets on the original entry are preserved for comparison.
"""

import copy
import sys
import os

# Allow running from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from agents import RunResult  # noqa: E402

from api.assistant.agent import (  # noqa: E402
    _apply_naive_snippets,
    _build_conversation_text,
    CatalogSearchResult,
    ContentSearchResult,
)


_DIVIDER = "=" * 80
_SECTION_DIV = "-" * 60


def _page_range(s) -> str:
    start = s.start_page
    end = s.end_page
    if start is None and end is None:
        return "?"
    if start == end:
        return str(start)
    return f"{start}–{end}"


def _entry_info(entry, run_result) -> tuple:
    """Return (edition_id, title) for CatalogSearchResult or ContentSearchResult."""
    if isinstance(entry, CatalogSearchResult):
        return entry.orm_edition.id, getattr(
            entry.orm_work, "title", None
        ) or "(no title)"
    # ContentSearchResult — book info lives on the execution context
    frbr_fields = run_result.context_wrapper.context.frbr_fields
    return entry.edition_id, frbr_fields.get("title", "(no title)")


def display_snippet_comparison(run_result: RunResult) -> None:
    """
    Print a side-by-side debug view of:
      1. The conversation text passed to the snippet agent
      2. The snippets selected by the snippet agent (grouped by edition)
      3. The naive snippets produced by _apply_naive_snippets (grouped by edition)
    """
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        print("No search results found on run_result.")
        return

    # Use the last search result (same as agent.py:get_relevant_snippets)
    _, search_result = list(search_results.items())[-1]
    edition_data = search_result.get("edition_data", [])

    # ── SECTION 1: CONVERSATION TEXT ────────────────────────────────────────────
    conversation_text = _build_conversation_text(run_result)
    print(_DIVIDER)
    print("CONVERSATION TEXT")
    print(_DIVIDER)
    print(conversation_text if conversation_text else "(empty)")

    # ── SECTION 2: SELECTED SNIPPETS (snippet agent output) ─────────────────────
    print()
    print(_DIVIDER)
    print("SELECTED SNIPPETS  (snippet agent output, grouped by edition)")
    print(_DIVIDER)

    if not edition_data:
        print("(no edition data)")
    else:
        for entry in edition_data:
            edition_id, title = _entry_info(entry, run_result)
            print(f"\nEDITION {edition_id}  —  {title}")
            print(_SECTION_DIV)

            if not entry.snippets:
                print("  (no snippets selected)")
            else:
                sorted_snippets = sorted(
                    entry.snippets,
                    key=lambda s: s.chunk_score or 0,
                    reverse=True,  # higher first
                )
                for i, s in enumerate(sorted_snippets, 1):
                    score = f"{s.chunk_score:.4f}" if s.chunk_score is not None else "?"
                    print(
                        f"  [{i}] item_id={s.item_id}  page={_page_range(s)}  score={score}"
                    )
                    for line in s.text.splitlines():
                        print(f"      {line}")

    # ── SECTION 3: NAIVE SNIPPETS (_apply_naive_snippets on a copy) ──────────────
    print()
    print(_DIVIDER)
    print("NAIVE SNIPPETS  (_apply_naive_snippets on entry copy, grouped by edition)")
    print(_DIVIDER)

    if not edition_data:
        print("(no edition data)")
    else:
        for entry in edition_data:
            edition_id, title = _entry_info(entry, run_result)
            print(f"\nEDITION {edition_id}  —  {title}")
            print(_SECTION_DIV)

            if not entry.chunk_hits:
                print("  (no chunk hits)")
            else:
                # Shallow-copy the entry and clear snippets so _apply_naive_snippets
                # runs fresh without disturbing LLM-selected snippets on the original.
                naive_entry = copy.copy(entry)
                naive_entry.snippets = []
                _apply_naive_snippets(naive_entry)

                max_naive_snippets = 7
                sorted_snippets = sorted(
                    naive_entry.snippets,
                    key=lambda s: s.chunk_score or 0,
                    reverse=True,  # higher first
                )
                for i, s in enumerate(sorted_snippets[:max_naive_snippets], 1):
                    score = f"{s.chunk_score:.4f}" if s.chunk_score is not None else "?"
                    print(
                        f"  [{i}] item_id={s.item_id}  page={_page_range(s)}  score={score}"
                    )
                    for line in s.text.splitlines():
                        print(f"      {line}")

    print()
    print(_DIVIDER)
