from dataclasses import dataclass, field, asdict
import difflib
import json
import re
import asyncio
from typing import (
    Dict,
    Any,
    Literal,
    Optional,
    Union,
    List,
    Iterator,
    Callable,
    Tuple,
    TypeAlias,
)
from typing_extensions import TypedDict
from enum import Enum
from textwrap import indent
import sys
import os
import asyncio
import time
from pathlib import Path

from agents import (
    RunResult,
)
from agents.items import ToolCallOutputItem
from openai import AsyncOpenAI
from pydantic import BaseModel
from jinja2 import Template
from rapidfuzz import fuzz
import rapidfuzz

# api code
from ..event_loop import run_coroutine
from ..utils import APIUtils, remove_markdown_comments, shorten
from .types import Snippet, BaseEditionResult
from .agent import format_search_results, TOOL_ERROR_PREFIX

# shared code
from utils.timer import timer
from logger import create_log

logger = create_log(__name__)


PROMPTS_DIR = Path(__file__).parent / "prompts"

SnippetMatcher: TypeAlias = Callable[[str, str], tuple[Optional[str], Optional[float]]]


# UNUSED
def tight_match_ellipsis(
    snippet_text: str, chunk_text: str
) -> tuple[Optional[str], Optional[float]]:
    """Match the generated snippet to chunk text.

    Returns (resolved_snippet, 1.0) if match is found, else (None, None).

    Matching Algo: the resolved snippet is the first whitespace-normalized exact
    string match of snippet_text in chunk_text.

    If snippet_text contains the elision token ``//...//``, it is split into lead
    and trail, which are matched against the chunk with a pattern requiring at
    least one character bridging them. Lead and trail must not be empty to match.
    """
    # NOTE: Since snippet text is indented in search tool response to LLM, we \
    # collapse all whitespace (including tabs) to single space.

    # elided snippet
    # TODO: guard against or handle multiple elisions
    if "//...//" in snippet_text:
        parts = snippet_text.split("//...//", 1)
        lead_tokens = parts[0].split()
        trail_tokens = parts[1].split()
        if not lead_tokens or not trail_tokens:
            return None, None
        lead_pat = r"\s+".join(re.escape(t) for t in lead_tokens)
        trail_pat = r"\s+".join(re.escape(t) for t in trail_tokens)
        pattern = lead_pat + r"\s+.+?\s+" + trail_pat

    # plain snippet
    else:
        tokens = snippet_text.split()
        if not tokens:
            return None, None
        pattern = r"\s+".join(re.escape(t) for t in tokens)

    m = re.search(pattern, chunk_text, re.DOTALL)
    return (m.group(0), 1.0) if m else (None, None)


def text_processor(s):
    # TODO: add custom processor that collapses "-\n"->""
    # MAYBE: ascii folding
    # set all non word, digit, whitespace to empty str
    s = re.sub(r"[^\w\d\s]", "", s)
    # collapse one or more whitespace char to single space
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    return s


def fuzzy_match(snippet, chunk):
    """
    Edit distance of best match substring

    Start and end in return value are in post-processed text strings
    """

    r = fuzz.partial_ratio_alignment(snippet, chunk, processor=text_processor)
    return {"score": r.score, "start": r.dest_start, "end": r.dest_end}


def fuzzy_match_ellipsis(snippet_text, chunk_text, threshold=88):
    """Match the generated snippet to chunk text.

    Splits snippet by `...` and match each part in sequence.

    Returns (snippet_text, min_part_score) if match is found, else (snippet_text, None).

    Part Match Algo: remove alpha num chars, collapse+trim whitespace, lowercase
    then check if best chunk substring edit distance is above threshold.
    """
    # MAYBE: text_process all at start
    parts = [p for p in snippet_text.strip("...").split("...") if text_processor(p)]
    if len(parts) == 0:
        return snippet_text, None
    results = [fuzzy_match(part, chunk_text) for part in parts]
    scores = [r["score"] for r in results]
    all_match = all(s >= threshold for s in scores)
    # NOTE: if there are multiple matches some sequential and some not, it is
    # possible fuzz would return a match that is not sequential even if there is
    # a sequential match.
    # NOTE: partial_ratio dest_end may extend beyond final matching block (bc
    # of matching block selection algo)
    all_sequential = (
        all(
            results[i]["end"] <= results[i + 1]["start"]
            for i in range(len(results) - 1)
        )
        if all_match
        else False
    )

    if all_match and all_sequential:
        return snippet_text, min(scores)
    else:
        return snippet_text, None


def _format_no_match_diff(snippet_text: str, chunk_hits: list) -> str:
    """Return a human-readable word-level diff between the submitted snippet
    and the closest matching region found across all chunk hits.

    Uses SequenceMatcher to locate the best-aligned window inside each chunk,
    then formats a word-level ndiff so it's easy to spot where the LLM
    misquoted the source text.
    """
    snippet_norm = " ".join(snippet_text.split())
    snippet_words = snippet_norm.split()
    if not snippet_words:
        return "  (empty snippet)"

    best_ratio = -1.0
    best_chunk_window_words: list[str] = []

    for chunk_hit in chunk_hits:
        chunk_norm = " ".join(chunk_hit.get("text", "").split())
        chunk_words = chunk_norm.split()
        if not chunk_words:
            continue

        # work token diff
        sm = difflib.SequenceMatcher(None, snippet_words, chunk_words, autojunk=False)
        ratio = sm.ratio()

        # Locate the region of the chunk best aligned with the snippet.
        # Pad by a few words on each side so trailing/leading insertions are visible.
        real_blocks = [b for b in sm.get_matching_blocks() if b.size > 0]
        if not real_blocks:
            continue
        WINDOW_PAD = 5
        chunk_start = max(0, real_blocks[0].b - WINDOW_PAD)
        chunk_end = min(
            len(chunk_words), real_blocks[-1].b + real_blocks[-1].size + WINDOW_PAD
        )
        window_words = chunk_words[chunk_start:chunk_end]

        if ratio > best_ratio:
            best_ratio = ratio
            best_chunk_window_words = window_words

    # No matching token blocks in any chunk
    if not best_chunk_window_words:
        return f"  snippet (normalized): {repr(snippet_norm[:300])}"

    diff_lines = list(difflib.ndiff(snippet_words, best_chunk_window_words))

    lines = [
        f"  best chunk similarity: {best_ratio:.1%}",
        "  word diff - each line is a word token  (- submitted by LLM, + actual text in chunk):",
    ]
    lines.extend(f"    {line}" for line in diff_lines)
    return "\n".join(lines)


class RejectionCode(str, Enum):
    SNIPPET_COUNT = "SNIPPET_COUNT"
    NO_MATCH = "NO_MATCH"
    WORD_LIMIT = "WORD_LIMIT"


@dataclass
class Rejection:
    edition_id: int
    snippet: Optional[str]
    code: RejectionCode
    data: dict = field(default_factory=dict)


def build_rejection_message(rejected: List[Rejection]) -> str:
    """Build the full rejection response string from a list of Rejection objects."""

    def _snippet_preview(r: Rejection) -> str:
        if not r.snippet:
            return ""
        s = r.snippet[:60] + ("..." if len(r.snippet) > 60 else "")
        return f"snippet {repr(s)}: "

    _MESSAGES: dict[RejectionCode, Callable[[Rejection], str]] = {
        RejectionCode.SNIPPET_COUNT: lambda r: (
            "0 snippets submitted; please submit at least one snippet."
        ),
        RejectionCode.NO_MATCH: lambda r: (
            """Submitted snippet text was not able to be matched to any chunk. 
Ensure the text is copied character by character from the chunk"""
            + (
                f"""Here is a word-level diff between your submitted snippet and the closest matching chunk. Use this diff to identify how you need to correct your snippet.
{r.data["diff"]}"""
                if r.data.get("diff")
                else ""
            )
            # ", if using elision, lead/trail are copied verbatim and that non-empty text appears on both sides of '//...//`'.
        ),
        RejectionCode.WORD_LIMIT: lambda r: (
            f"Resolved snippet has {r.data['word_count']} words, exceeding the "
            f"150-word hard limit (100-word target + 50-word grace margin). "
            f"Please shorten or use tighter elision."
        ),
    }

    blocks = [
        f"""{len(rejected)} snippet(s) did not pass validation.
Follow the instructions below for guidance on what to resubmit and how to correct your submission."""
    ]
    for r in rejected:
        blocks.append(
            f"  REJECTED SNIPPET:{_snippet_preview(r)}\n{indent(_MESSAGES[r.code](r), '  ')}"
        )
    blocks.append(
        "In your next response, include ONLY the corrected snippets listed above."
    )
    return "\n\n".join(blocks)


class _SnippetSelectionResponse(BaseModel):
    snippets: List[str]


def validate_edition_snippets(
    edition_id: int,
    snippet_list: List[str],
    chunk_hits: list,
    find_snippet_in_chunk: SnippetMatcher = tight_match_ellipsis,
) -> Tuple[List[Rejection], List[Snippet]]:
    """Validate a list of submitted snippets against the chunk hits of one edition.

    Pure function — no side effects. Returns (rejections, validated_snippets) where
    validated_snippets are fully-formed Snippet objects ready to extend an entry's .snippets.
    This is the natural unit for future parallel-edition processing.

    Args:
        edition_id: The edition being validated (used only for rejection messages).
        snippet_list: Submitted snippet strings.
        chunk_hits: The chunk hits stored for this edition.
        find_snippet_in_chunk: Callable(snippet_text, chunk_text) -> (resolved_snippet, score)
            where score is a float on match or None on no match.

    Returns:
        Tuple of (list of Rejection objects, list of valid Snippet objects).
    """
    rejections: List[Rejection] = []
    validated: List[Snippet] = []

    # --- Guard: reject if LLM submitted zero snippets ---
    # NOTE: if we want to make this a substantive minimum snippets per edition \
    # validation, we need to either pull the check into the caller or pass the \
    # entry.snippets state into the function to insure it is a cumulative minimum \
    # per edition over multiple runs.
    if not snippet_list:
        logger.debug(f"Rejecting edition {edition_id}: 0 snippets submitted.")
        return (
            [Rejection(edition_id, None, RejectionCode.SNIPPET_COUNT, {"count": 0})],
            [],
        )

    for snippet_text in snippet_list:
        # --- Step 1: Validate snippet matches text of a chunk ---
        # Match snippet to chunk with highest match score (as calculated by find_snippet_in_chunk). In the case of ties the first chunk wins (strict >).
        best_chunk_hit = None
        best_resolved_snippet = None
        best_score = -1.0
        for chunk_hit in chunk_hits:
            _resolved, _score = find_snippet_in_chunk(
                snippet_text, chunk_hit.get("text", "")
            )
            if _score is not None and _score > best_score:
                best_score = _score
                best_chunk_hit = chunk_hit
                best_resolved_snippet = _resolved
        if best_chunk_hit is not None:
            logger.debug(
                f"Best chunk match score {best_score:.2f} for snippet '{snippet_text[:60]}'"
            )
        matched_chunk, resolved_snippet = best_chunk_hit, best_resolved_snippet

        if matched_chunk is None:
            diff_log = _format_no_match_diff(snippet_text, chunk_hits)
            logger.debug(
                f"Rejecting snippet for edition {edition_id}: not found in any chunk.\n"
                f"  submitted snippet: {repr(snippet_text[:80])}\n"
                f"{diff_log}"
            )
            rejections.append(
                Rejection(
                    edition_id, snippet_text, RejectionCode.NO_MATCH, {"diff": diff_log}
                )
            )
            continue

        # --- Step 2: Validate word count on the resolved snippet ---
        word_count = len(resolved_snippet.split())
        if word_count > 150:
            logger.debug(
                f"Rejecting snippet for edition {edition_id}: resolved snippet "
                f"has {word_count} words, exceeding 150-word hard limit."
            )
            rejections.append(
                Rejection(
                    edition_id,
                    snippet_text,
                    RejectionCode.WORD_LIMIT,
                    {"word_count": word_count},
                )
            )
            continue

        # All validations passed — build the storable snippet
        validated.append(
            Snippet(
                text=resolved_snippet,
                start_page=matched_chunk.get("start_page")
                or matched_chunk.get("chunk_start_page"),
                end_page=matched_chunk.get("end_page")
                or matched_chunk.get("chunk_end_page"),
                item_id=matched_chunk.get("item_id"),
                chunk_score=matched_chunk.get("score")
                or matched_chunk.get("meta", {}).get("score"),
            )
        )

    return rejections, validated


def format_conversation_history(
    items: list, preserve_last_output: bool = False
) -> str:  # list[TResponseOutputItem]
    """Format conversation history from a list of OpenAI Responses API items.

    - All final tool call outputs except the final one are summarized as "N results returned"
      by counting <edition> elements in the returned XML, or an error message if error.
    - Within each agent turn (a run of consecutive tool call/output pairs between
      message items), only the final complete pair is kept.
    - The last tool call output in the entire messages list is summarized like the
      rest, unless preserve_last_output is True, in which case its full content is
      preserved as-is.
    - Items types other than "mesagage", "function_call", and "function_call_output"
      are skipped.
    """
    # MAYBE: keeping only the final tool call pair is over-complicated. Just keep all tool calls.

    from lxml import etree as ET

    # FUTURE: after upgrading agents sdk use this instead: https://openai.github.io/openai-agents-python/ref/items/#agents.items.ItemHelpers.extract_text
    def _get_msg_text(msg):
        content = msg.get("content", "")
        if isinstance(content, list):
            return "".join(
                part.get("text", "")
                for part in content
                if part.get("type")
                in (
                    "output_text",
                    "input_text",
                )  # input_text (EasyInputMessageParam / ResponseInputTextParam), output_text (ResponseOutputMessageParam / ResponseOutputText)
            )
        return str(content)

    def _compact_tool_output(output: str) -> str:
        """Summarize search tool output with a count of N results returned"""
        if output.startswith(TOOL_ERROR_PREFIX):
            return TOOL_ERROR_PREFIX
        try:
            root = ET.fromstring(output)
            # MAYBE: tighter criteria <edition> direct children of <search_results> according to output format
            count = len(root.findall("edition"))
            return f"{count} results returned"
        except ET.XMLSyntaxError:
            return output

    # Hold a reference to the last function_call_output item so we can preserve
    # its full output without summarization.
    last_output_item = None
    for msg in reversed(items):
        if msg.get("type") == "function_call_output":
            last_output_item = msg
            break

    parts = []
    # Buffer of (function_call_item, function_call_output_item | None) pairs
    # accumulated between message items. Flushed on each message item, keeping
    # only the last complete pair.
    tool_buffer: list[tuple[dict, Optional[dict]]] = []
    call_id_to_idx: dict[str, int] = {}

    def flush_tool_buffer():
        for call_item, output_item in reversed(tool_buffer):
            if output_item is not None:
                tool_name = call_item.get("name", "tool")
                raw_output = output_item.get("output", "")
                if output_item is last_output_item and preserve_last_output:
                    output_text = raw_output
                else:
                    output_text = _compact_tool_output(raw_output)
                parts.append(f"[Tool: {tool_name}]\n{output_text}")
                break
        tool_buffer.clear()
        call_id_to_idx.clear()

    for msg in items:
        msg_type = msg.get("type")
        role = msg.get("role", "")

        if msg_type == "function_call":
            idx = len(tool_buffer)
            tool_buffer.append((msg, None))
            call_id = msg.get("call_id")
            if call_id:
                call_id_to_idx[call_id] = idx

        elif msg_type == "function_call_output":
            call_id = msg.get("call_id")
            if call_id in call_id_to_idx:
                idx = call_id_to_idx[call_id]
                call_item, _ = tool_buffer[idx]
                tool_buffer[idx] = (call_item, msg)
            else:
                # Orphaned output with no matching function_call
                tool_buffer.append(({"name": "unknown", "call_id": call_id}, msg))

        elif msg_type == "message" or (
            msg_type is None and role in ("user", "assistant")
        ):
            if tool_buffer:
                flush_tool_buffer()
            text = _get_msg_text(msg).strip()
            if text and role in ("user", "assistant"):
                label = "User" if role == "user" else "Assistant"
                parts.append(f"{label}: {text}")

        # reasoning items and other types are skipped

    if tool_buffer:
        flush_tool_buffer()

    return "\n\n".join(parts)


def _extract_tool_output(run_result: RunResult, tool_call_id: str) -> Optional[str]:
    """Find the string output of a tool call in run_result.new_items by tool call id.

    Returns None if the matching ToolCallOutputItem is not found.
    """
    for item in run_result.new_items:
        if not isinstance(item, ToolCallOutputItem):
            continue
        raw = item.raw_item
        call_id = (
            raw.get("call_id") if hasattr(raw, "get") else getattr(raw, "call_id", None)
        )
        if call_id == tool_call_id:
            return str(item.output)
    return None


def _apply_naive_snippets(entry: "BaseEditionResult") -> None:
    """Append a naive truncated snippet for every chunk_hit in entry.

    Does not skip entries that already have snippets.
    """
    for chunk_hit in entry.chunk_hits:
        entry.snippets.append(
            Snippet(
                text=shorten(chunk_hit["text"]),
                start_page=chunk_hit.get("start_page")
                or chunk_hit.get("chunk_start_page"),
                end_page=chunk_hit.get("end_page") or chunk_hit.get("chunk_end_page"),
                item_id=chunk_hit.get("item_id"),
                chunk_score=chunk_hit.get("score")
                or chunk_hit.get("meta", {}).get("score"),
            )
        )


@timer(logger)
def get_relevant_snippets_naive(run_result: RunResult) -> Optional[bool]:
    """Naively populate snippets for all editions in the last search result using
    simple text truncation — no AI selection.

    Same input/output/side-effect shape as get_relevant_snippets: returns None if
    no search results, True on success, and updates search_results in place.
    Available as a lightweight fallback or for offline use.
    """
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        return None

    _, search_result = list(search_results.items())[-1]
    edition_data = search_result.get("edition_data", [])

    for entry in edition_data:
        if entry.snippets:
            continue
        _apply_naive_snippets(entry)

    return True


_SNIPPET_AGENT_MAX_TURNS = 1
_SNIPPET_AGENT_MAX_CONCURRENT = 20


class EditionSnippetLoop:
    """Self-validating LLM snippet selection loop for a single edition.

    Instantiate with the loop configuration, then call run() to execute.
    Agent instruction are passed in the form of messages = (system + initial user turn).

    """

    def __init__(
        self,
        client: AsyncOpenAI,
        model_name: str,
        messages: list,
        entry: BaseEditionResult,
    ) -> None:
        self.client = client
        self.model_name = model_name
        self.messages = messages
        self.entry = entry
        self.model_config: dict = {"temperature": 0.0, "reasoning_effort": "none"}
        self.last_response = None
        self.n_turns: int = 0
        self.max_turns_exceeded: bool = False

    async def run(self) -> None:
        """Run the snippet selection loop.

        Modifies entry.snippets in place on success.
        Sets max_turns_exceeded=True if all turns are exhausted without a fully
        valid submission. last_response and n_turns are updated incrementally
        and can be inspected if this method raises.
        """
        for turn in range(_SNIPPET_AGENT_MAX_TURNS):
            # Get LLM response
            t0 = time.perf_counter()  # TODO: use Timer()
            # MAYBE: Add edition_id or entry as Loop context
            response = await self.client.chat.completions.parse(
                model=self.model_name,
                messages=self.messages,
                response_format=_SnippetSelectionResponse,
                # equivalent to run_config.model_settings
                **self.model_config,
            )
            self.last_response = response
            self.n_turns = turn + 1
            elapsed = time.perf_counter() - t0
            usage = response.usage
            logger.info(
                f"Snippet agent edition {self.entry.edition_id}"
                f" | turn {self.n_turns}/{_SNIPPET_AGENT_MAX_TURNS}"
                f" | elapsed: {elapsed:.3f}s"
                f" | finish_reason: {response.choices[0].finish_reason}"
                f" | refusal: {response.choices[0].message.refusal}"
                + (
                    f" | tokens: input={usage.prompt_tokens} output={usage.completion_tokens}"
                    if usage
                    else ""
                )
            )

            # Guard: non-stop finish reasons (e.g. RECITATION content filter) are unrecoverable
            choice = response.choices[0]
            if choice.finish_reason != "stop":
                raise RuntimeError(
                    f"Snippet agent edition {self.entry.edition_id}: non-stop finish_reason"
                    f" '{choice.finish_reason}' (refusal: {choice.message.refusal})"
                )

            # Extract structured output
            parsed = choice.message.parsed
            if parsed is None:
                logger.debug(
                    f"Snippet agent edition {self.entry.edition_id}: structured output is `None` on turn {self.n_turns}; treating as []."
                )
                snippet_list = []
            else:
                snippet_list = parsed.snippets

            logger.info(
                f"Snippet agent edition {self.entry.edition_id}: {len(snippet_list)} snippet(s) submitted for validation,"
                f" {len(self.entry.chunk_hits)} chunk(s) available."
            )

            # Validate selected snippets
            rejections, validated = validate_edition_snippets(
                self.entry.edition_id,
                snippet_list,
                self.entry.chunk_hits,
                find_snippet_in_chunk=fuzzy_match_ellipsis,
            )
            # Save valid snippets
            # MAYBE: consider not setting result in place
            self.entry.snippets.extend(validated)
            if validated:
                for s in validated:
                    snippet_lead = repr(s.text[:50]).strip("'\"")
                    logger.debug(
                        f"Snippet agent edition {self.entry.edition_id}: saved snippet '{snippet_lead}...'"
                    )
                logger.info(
                    f"Snippet agent edition {self.entry.edition_id}: {len(validated)} snippet(s) saved (turn {self.n_turns})."
                )

            if not rejections:
                return

            logger.info(
                f"Snippet agent edition {self.entry.edition_id}: {len(rejections)} rejection(s) on turn {self.n_turns}."
            )

            # Request corrections
            # MAYBE: update system-prompt/first-message rather than extending convo
            rejection_message = build_rejection_message(rejections)
            assistant_content = choice.message.content or (
                json.dumps(parsed.model_dump()) if parsed is not None else ""
            )
            self.messages.append({"role": "assistant", "content": assistant_content})
            self.messages.append({"role": "user", "content": rejection_message})

        logger.error(
            f"Snippet agent edition {self.entry.edition_id} failed to correctly submit all originally submitted snippets after {_SNIPPET_AGENT_MAX_TURNS} turns."
        )
        self.max_turns_exceeded = True


@timer(logger)
def get_relevant_snippets_llm(
    run_result: RunResult,
    fallback_naive: bool = True,
) -> Optional[List[EditionSnippetLoop]]:
    """Use LLM to select relevant snippets from the last
    search result in run_result.
    LLM generates full text of snippet. Uses iterative self-validating loop to
    ensure accuracy of generated snippet.

    Updates run_result.context_wrapper.context.search_results in place.

    Runs snippet selection on each edition in search result in parallel.

    Returns None if no search results, [] if all editions already had snippets,
    or a list of EditionSnippetLoop (one per edition processed)

    Args:
        fallback_naive: If True (default), editions with no AI-selected snippets
            fall back to naive truncated snippets. Set False to disable the
            fallback.
    """
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        logger.warning(
            "get_relevant_snippets: no search results found in agent run_result."
        )  # TODO: deduplicate, also  logged in format_search_results
        return None

    # Select last search result
    _, search_result = list(search_results.items())[-1]

    # Skip editions that already have snippets
    # TODO: figure out how to add preexisting snippet editions to logging and aggregation at end, maybe no early return
    edition_data = []
    for e in search_result.get("edition_data", []):
        if e.snippets:
            logger.info(
                f"get_relevant_snippets: edition {e.edition_id} already has {len(e.snippets)} snippet(s); skipping."
            )
        else:
            edition_data.append(e)
    if not edition_data:
        logger.info("get_relevant_snippets: all editions already have snippets.")
        return []

    # Shared model config
    client: AsyncOpenAI = run_result.last_agent.model._client
    model_name: str = run_result.last_agent.model.model
    # model_name = "gemini-3.1-flash-lite-preview"
    # model_name = 'gemini-2.5-flash-lite'

    # Shared system prompt variables
    conversation_text = format_conversation_history(
        run_result.to_input_list(), preserve_last_output=True
    )
    prompt_template = Template(
        (PROMPTS_DIR / "snippet_agent" / "v7.jinja.md").read_text()
    )

    # Build edition specific instructions
    loops: List[EditionSnippetLoop] = []
    for entry in edition_data:
        # Format search result chunk text
        # NOTE: slowish. in some cases constructing a 54,813 token str from 100 chunks.
        edition_chunk_text = format_search_results([entry])

        snippet_agent_prompt = remove_markdown_comments(
            prompt_template.render(
                system_prompt=run_result.last_agent.instructions,
                search_tool_name=run_result.last_agent.tools[0].name,
                search_tool_description=run_result.last_agent.tools[0].description,
                conversation_text=conversation_text,
                search_result_text=edition_chunk_text,
            )
        )
        # TODO: test that the main agent has only 1 tool (this is assumed in writing
        # the relevant snippet prompt)
        messages = [
            {"role": "system", "content": snippet_agent_prompt},
            {"role": "user", "content": "Begin snippet selection."},
        ]

        loop = EditionSnippetLoop(client, model_name, messages, entry)
        loops.append(loop)

    # Run edition snippet selection in parallel

    async def _run_all():
        # Semaphore/gather must be constructed while the target loop is actually
        # running (i.e. inside this coroutine), not in the calling sync context —
        # otherwise they bind to the wrong (or no) event loop.
        semaphore = asyncio.Semaphore(_SNIPPET_AGENT_MAX_CONCURRENT)

        async def _gated(coro):
            """max concurrency wrapper"""
            async with semaphore:
                return await coro

        # TODO: refactor to use sync OpenAIClient, make loop.run() sync, and \
        # use ThreadPoolExecutor since we are not (yet) using a async asgi app.
        return await asyncio.gather(
            *(_gated(l.run()) for l in loops), return_exceptions=True
        )

    logger.info(
        f"get_relevant_snippets: running snippet agent for {len(loops)} edition(s) concurrently"
        f" (max {_SNIPPET_AGENT_MAX_CONCURRENT} at a time)."
    )
    # MAYBE: handle tokens per minute rate limit errors explicitly with exponential backoff?
    results = run_coroutine(_run_all())

    # Log results + Apply fallback snippets
    n_errored = 0  # raised an exception
    n_incomplete = 0  # hit max turns with at least one invalid submission
    n_no_snippets = 0  # zero AI-selected snippets (fell back to naive)
    total_snippets = 0
    for loop, entry, result in zip(loops, edition_data, results):
        if isinstance(result, Exception):
            logger.exception(
                f"Snippet agent edition {entry.edition_id}: raised an exception"
                f" after {loop.n_turns} turn(s).",
                exc_info=result,
            )
            n_errored += 1
        elif loop.max_turns_exceeded:
            n_incomplete += 1

        # Fallback to naive snippets if selection failed
        selected_count = len(entry.snippets)
        if not entry.snippets:
            n_no_snippets += 1
            if fallback_naive:
                logger.warning(
                    f"Snippet agent edition {entry.edition_id}: no snippets saved; "
                    f"falling back to naive snippets."
                )
                _apply_naive_snippets(entry)
            else:
                logger.warning(
                    f"Snippet agent edition {entry.edition_id}: no snippets saved "
                    f"(naive fallback disabled)."
                )
        else:
            logger.info(
                f"Snippet agent edition {entry.edition_id}: {selected_count} AI-selected snippet(s) saved"
                f" in {loop.n_turns} turn(s)."
            )
        total_snippets += selected_count

    logger.info(
        f"get_relevant_snippets: Edition summary: {len(edition_data)} processed"
        f" | {n_no_snippets} with no snippets saved"
        f" | {n_incomplete} invalid snippets after max turns"
        f" | {n_errored} errored"
    )
    logger.info(f"get_relevant_snippets: {total_snippets} total AI-selected snippet(s)")

    return loops


def get_relevant_snippets(
    run_result: RunResult,
    approach: Literal["llm", "naive"] = "naive",
    **kwargs,
):
    if approach == "llm":
        return get_relevant_snippets_llm(run_result, **kwargs)
    return get_relevant_snippets_naive(run_result)
