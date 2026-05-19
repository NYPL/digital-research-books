#!/usr/bin/env python
"""Run retrieval-only LLM-as-judge eval across multiple embedding indices.

Uses DeepEval's ContextualRelevancyMetric against retrieved chunks without any
predefined expected answers/chunk labels.

Example:
uv run dev_scripts/embedding/llm_judge_retrieval.py \
   --top-k 2 \
   --indices vra_test-eval300-gemini_001 vra_test-eval300-qwen3_embedding_4b \
   --query "lyric verse sublime nature pastoral landscape beauty" \
   --query "rapid urbanization social structures 19th century Europe industrial revolution class changes city growth"

Docs: https://deepeval.com/docs/metrics-contextual-relevancy
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import find_dotenv

# Compute project root
project_root = Path(
    find_dotenv("requirements.txt", raise_error_if_not_found=True)
).parent

if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from vector_indexing.core.config import get_index_config


def build_judge(model_name: str):
    """Build a DeepEval judge model.

    Gemini uses GPTModel with a custom base_url.
    OpenAI models (gpt-4o, gpt-4.1, etc.) are passed as plain strings.
    """
    if model_name.startswith("gemini"):
        import os
        from deepeval.models import GPTModel  # type: ignore[import-not-found]

        return GPTModel(
            model=model_name,
            api_key=os.environ["GOOGLE_API_KEY"],
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
            temperature=0,  # without temprature=0, I found relatively high variability in results
        )
    # TODO: set temp 0 for non gemini models
    return model_name


EVAL300_INDICES = [
    "vra_test-eval300-gemini_001",  # pragma: allowlist secret
    "vra_test-eval300-harrier_oss_v1_.6b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_8b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_4b",  # pragma: allowlist secret
    "vra_test-eval300-pplx_embed_v1_4b",  # pragma: allowlist secret
]

DEFAULT_QUERIES_FILE = Path(__file__).parent / "ranking_task_queries.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run retrieval-only DeepEval ContextualRelevancyMetric across one or more "
            "eval300 embedding indices."
        )
    )
    parser.add_argument(
        "--indices",
        nargs="+",
        default=None,
        help="Index names to evaluate. If omitted, uses all known eval300 indices.",
    )
    parser.add_argument(
        "--query",
        action="append",
        default=None,
        help="Add a query (repeatable). If omitted, uses built-in diverse starter queries.",
    )
    parser.add_argument(
        "--queries-file",
        type=Path,
        default=None,
        help="File with one query per line (empty lines and # comments ignored). "
        f"Defaults to ranking_task_queries.txt in the same directory as this script.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Number of retrieved chunks per query/index (default: 20).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=1,  # forces deeper output if using default printing
        help="DeepEval contextual relevancy pass threshold (default: 0.7).",
    )
    parser.add_argument(
        "--judge-model",
        type=str,
        default="gemini-3.1-pro-preview",
        help="Gemini model name (default: gemini-3.1-pro-preview) or 'gpt-4o' etc. for OpenAI.",
    )
    parser.add_argument(
        "--out-json",
        type=Path,
        default=None,
        help="Path to write detailed JSON results (default: timestamped file in llm_as_judge_results/).",
    )
    parser.add_argument(
        "--suppress-stmts",
        action="store_true",
        help="Suppress per-chunk statement verdicts (printed by default).",
    )
    parser.add_argument(
        "--env",
        default="production",
        help="Environment name used to load config/.env.<env> (default: production).",
    )
    return parser.parse_args()


def load_queries(args: argparse.Namespace) -> list[str]:
    def _read_queries_file(path: Path) -> list[str]:
        return [
            stripped
            for line in path.read_text().splitlines()
            if (stripped := line.strip()) and not stripped.startswith("#")
        ]

    # TODO: make queries and queries-file mutually exclusive
    queries: list[str] = []
    if args.query:
        queries.extend([q.strip() for q in args.query if q.strip()])
    if args.queries_file:
        queries_file = (
            args.queries_file
            if args.queries_file.is_absolute()
            else project_root / args.queries_file
        )
        if not queries_file.exists():
            raise FileNotFoundError(f"queries file not found: {queries_file}")
        queries.extend(_read_queries_file(queries_file))
    if not queries:
        if not DEFAULT_QUERIES_FILE.exists():
            raise FileNotFoundError(
                f"Default queries file not found: {DEFAULT_QUERIES_FILE}. "
                "Provide queries via --query or --queries-file."
            )
        queries.extend(_read_queries_file(DEFAULT_QUERIES_FILE))
    return queries


def summarize_distances(
    distances: list[float | None],
) -> tuple[float | None, float | None, float | None]:
    valid = [d for d in distances if isinstance(d, (int, float))]
    if not valid:
        return None, None, None
    return statistics.mean(valid), min(valid), max(valid)


def build_test_case(*, index_name: str, query: str, top_k: int) -> "LLMTestCase":
    """Search the index for the query and return an LLMTestCase.

    All data needed for printing is stored in metadata, which deepeval propagates to TestResult.metadata.
    """
    from deepeval.test_case import LLMTestCase  # type: ignore[import-not-found]

    index_config = get_index_config(index_name)
    embedder = index_config["embedder"]
    backend = index_config["backend"]

    query_vector = embedder.embed_query(query)
    search_results = backend.query(
        rank_by=("vector", "ANN", query_vector),
        top_k=top_k,
        include_attributes=["text"],
    )

    mean_dist, min_dist, max_dist = summarize_distances(
        [dist for _, dist in search_results]
    )

    # Score = Number of Relevant Statements / Total Number of Statements.
    # The use of "statements" suggests this metric is designed more for fact-based
    # retrieval than more generally applicable "relevance" (genre, theme, style, topic).
    return LLMTestCase(
        input=query,  # ALT: use update_chat() to generate TP queries via search tools
        retrieval_context=[chunk.text.strip() for chunk, _ in search_results],
        metadata={
            "index_name": index_name,
            "query": query,
            "retrieved_count": len(search_results),
            "top_k": top_k,
            "mean_distance": mean_dist,
            "min_distance": min_dist,
            "max_distance": max_dist,
            "chunks": [
                {
                    "title": chunk.book_metadata.title,
                    "doc_id": chunk.doc_id,
                    "dist": dist,
                }
                for chunk, dist in search_results
            ],
        },
    )


def parse_verdicts_from_verbose_logs(verbose_logs: str | None) -> list[dict] | None:
    """Parse per-chunk verdicts from MetricData.verbose_logs.

    deepeval stores verbose_logs as: "Verdicts:\\n<json_array>" where the array
    contains one ContextualRelevancyVerdicts dict per retrieval_context chunk,
    serialized via prettify_list (json.dumps + indent). This is always populated
    regardless of verbose_mode — verbose_mode only controls whether it's printed.
    """
    # NOTE: a bit brittle but seems like the only way to access ContextualRelevancyMetric.verdict_list from evaluate() output
    if not verbose_logs:
        return None
    prefix = "Verdicts:\n"
    if not verbose_logs.startswith(prefix):
        # TODO: raise error
        return None
    try:
        return json.loads(verbose_logs[len(prefix) :])
    except json.JSONDecodeError:
        return None


def print_stmt_verdicts(chunks_meta: list[dict], verdicts_list: list[dict]) -> None:
    """Print relevancy for each statement in chunk."""
    for chunk_idx, (chunk_meta, chunk_verdicts_dict) in enumerate(
        zip(chunks_meta, verdicts_list), start=1
    ):
        verdicts = chunk_verdicts_dict.get("verdicts", [])
        chunk_relevance = sum(
            1 for v in verdicts if v.get("verdict", "").lower() == "yes"
        )
        total = len(verdicts)
        chunk_score = chunk_relevance / total if total else 0.0
        dist = chunk_meta.get("dist")
        dist_str = f"{dist:.4f}" if dist is not None else "?"
        print(
            f"    Chunk {chunk_idx:02d}  [{chunk_relevance}/{total} relevant, score={chunk_score:.2f}]"
            f"  {chunk_meta.get('title', '?')}  |  id={chunk_meta.get('doc_id', '?')}  |  dist={dist_str}"
        )
        for stmt_idx, v in enumerate(verdicts, start=1):
            is_relevant = v.get("verdict", "").lower() == "yes"
            label = "RELEVANT  " if is_relevant else "IRRELEVANT"
            print(f"      stmt {stmt_idx:02d} [{label}]  {v.get('statement', '')}")
            reason = v.get("reason")
            # TODO: reason should always be printed, even if empty ( want to know if no reason is given)
            if reason and not is_relevant:
                print(f"               reason: {reason}")
        print()


def print_results(
    evaluation_result,
    suppress_stmts: bool,
) -> None:
    """Print per-index, per-query search result evaluation output, grouped Index > Query > Chunk > Statement."""
    # Group results by index, preserving per-index query order
    by_index: dict[str, list] = {}
    for tr in evaluation_result.test_results:
        index_name = (tr.metadata or {}).get("index_name", "?")
        by_index.setdefault(index_name, []).append(tr)

    for index_name, test_results in by_index.items():
        scores = [
            tr.metrics_data[0].score
            for tr in test_results
            if tr.metrics_data and tr.metrics_data[0].score is not None
        ]
        index_agg = (
            f"  mean_score={statistics.mean(scores):.4f}  "
            f"median_score={statistics.median(scores):.4f}  "
            f"n={len(scores)}"
            if scores
            else "  (no scores)"
        )
        print(f"\n{'═' * 70}")
        print(f"  INDEX: {index_name}")
        print(f"{index_agg}")
        print(f"{'═' * 70}")

        for tr in test_results:
            meta = tr.metadata or {}
            query = meta.get("query", tr.input)

            print(f"\n  QUERY: {query!r}")

            md = tr.metrics_data[0] if tr.metrics_data else None
            if md is None:
                print("  [ERROR] No metric data")
                continue

            status = "PASS" if md.success else "FAIL"
            print(
                f"  [{status}] score={md.score:.4f}"
                f"  retrieved={meta.get('retrieved_count', '?')}/{meta.get('top_k', '?')}"
            )
            if md.reason:
                print(f"  reason: {md.reason}")

            if not suppress_stmts:
                verdicts_list = parse_verdicts_from_verbose_logs(md.verbose_logs)
                chunks_meta = meta.get("chunks", [])
                if verdicts_list and chunks_meta:
                    print()
                    print_stmt_verdicts(chunks_meta, verdicts_list)


def write_results(
    evaluation_result,
    out_path: Path,
    indices: list[str],
    queries: list[str],
    args: argparse.Namespace,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    test_case_data = []
    for tr in evaluation_result.test_results:
        index_name = (tr.metadata or {}).get("index_name", tr.name)
        meta = tr.metadata or {}
        for md in tr.metrics_data or []:
            test_case_data.append(
                {
                    "index_name": index_name,
                    "query": meta.get("query", tr.input),
                    "score": md.score,
                    "passed": md.success,
                    "reason": md.reason,
                    "retrieved_count": meta.get("retrieved_count"),
                    "top_k": meta.get("top_k"),
                    "mean_distance": meta.get("mean_distance"),
                    "min_distance": meta.get("min_distance"),
                    "max_distance": meta.get("max_distance"),
                }
            )
    payload = {
        "indices": indices,
        "queries": queries,
        "config": {
            "top_k": args.top_k,
            "threshold": args.threshold,
            "judge_model": args.judge_model,
        },
        "test_case_data": test_case_data,
    }
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote detailed results to: {out_path}")


def main() -> None:
    from deepeval import evaluate
    from deepeval.evaluate.configs import DisplayConfig
    from deepeval.metrics import ContextualRelevancyMetric

    args = parse_args()

    from utils.load_env import load_env

    load_env(f"config/.env.{args.env}")

    if args.top_k <= 0:
        raise ValueError("--top-k must be > 0")

    indices = args.indices or EVAL300_INDICES
    if not indices:
        raise SystemExit("No indices found. Provide --indices explicitly.")

    queries = load_queries(args)
    if not queries:
        raise SystemExit("No queries provided or discovered.")

    out_path = args.out_json or (
        Path(__file__).parent
        / "llm_as_judge_results"
        / f"llm_eval_retrieval_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    )

    print(f"Evaluating indices ({len(indices)}): {indices}")
    print(f"Queries ({len(queries)}):")
    for i, q in enumerate(queries, start=1):
        print(f"  {i}. {q}")

    test_cases = [
        build_test_case(index_name=index_name, query=query, top_k=args.top_k)
        for index_name in indices
        for query in queries
    ]

    judge = build_judge(args.judge_model)
    # Score = Total Number of Statements/Number of Relevant Statements
    # NOTE: contextual relevancy is really a Precision metric (relevant/retrieved), \
    # so if a chunk matching all concepts from a query but includes additional \
    # info it is down scored, but if it includes a few of many concepts in a \
    # query but no other info is up rated....
    # TODO: we should look into a recall metric, that specifies the "concepts/statements" \
    # in the query and checks how many are addressed in each chunk, and an F-1 type score
    metric = ContextualRelevancyMetric(
        threshold=args.threshold,
        model=judge,
        include_reason=True,
        async_mode=True,
        verbose_mode=False,  # suppresses printing; verbose_logs is still populated
    )

    evaluation_result = evaluate(
        test_cases,
        [metric],
        display_config=DisplayConfig(
            print_results=False,
            verbose_mode=False,
        ),
    )

    # TODO: construct complete data in dict and write that as result (construct_json_data()). \
    # print function takes constructed serializable data, and prints that \
    # with options `verbosity: Literal['index', 'query', 'chunk', 'stmt']` to indicate \
    # what level of detail to print (default 'query')
    # include std dev in printing of index level scores, also include chunk score \
    # std deviation in query level display

    # ALT: use verbose mode for display config to use deepeval print structure
    print_results(evaluation_result, args.suppress_stmts)
    write_results(evaluation_result, out_path, indices, queries, args)


if __name__ == "__main__":
    main()
