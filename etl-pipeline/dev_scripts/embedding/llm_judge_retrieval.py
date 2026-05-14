#!/usr/bin/env python
"""Run retrieval-only LLM-as-judge eval across multiple embedding indices.

Uses DeepEval's ContextualRelevancyMetric against retrieved chunks without any
predefined expected answers/chunk labels.

Docs: https://deepeval.com/docs/metrics-contextual-relevancy
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import find_dotenv

# Compute project root
project_root = Path(
    find_dotenv("requirements.txt", raise_error_if_not_found=True)
).parent

# Add project root to path if not already presen
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from vector_indexing.core.config import get_index_config


def build_judge(model_name: str):
    """Build a DeepEval judge model.

    Gemini uses GPTModel with a custom base_url
    Requires GOOGLE_API_KEY in environment (loaded via load_env before this is called).
    OpenAI models (gpt-4o, gpt-4.1, etc.) are passed as plain strings.
    """
    if model_name.startswith("gemini"):
        import os
        from deepeval.models import GPTModel  # type: ignore[import-not-found]

        return GPTModel(
            model=model_name,
            api_key=os.environ["GOOGLE_API_KEY"],
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )
    return model_name


EVAL300_INDICES = [
    "vra_test-eval300-gemini_001",  # pragma: allowlist secret
    "vra_test-eval300-harrier_oss_v1_.6b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_8b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_4b",  # pragma: allowlist secret
    "vra_test-eval300-pplx_embed_v1_4b",  # pragma: allowlist secret
]

STARTER_QUERIES = [
    "How did public libraries support immigrant communities in early 20th-century U.S. cities?",
    "What are historical debates around censorship and banned books in school libraries?",
    "How have research libraries preserved and provided access to born-digital archives?",
    "What evidence exists about library programs improving digital literacy for older adults?",
    "How did library classification systems shape discoverability of marginalized authors?",
]


@dataclass
class EvalResult:
    index_name: str
    query: str
    score: float
    passed: bool
    reason: str | None
    retrieved_count: int
    top_k: int
    mean_distance: float | None
    min_distance: float | None
    max_distance: float | None


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
        help=("Index names to evaluate. If omitted, uses all known eval300 indices."),
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
        help="Optional file with one query per line (empty lines ignored).",
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
        default=0.7,
        help="DeepEval contextual relevancy pass threshold.",
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
        for line in queries_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                queries.append(line)
    if not queries:
        queries = STARTER_QUERIES.copy()
    return queries


def summarize_distances(
    distances: list[float | None],
) -> tuple[float | None, float | None, float | None]:
    valid = [d for d in distances if isinstance(d, (int, float))]
    if not valid:
        return None, None, None
    return statistics.mean(valid), min(valid), max(valid)


def print_stmt_verdicts(chunks: list[tuple], verdicts_list: list) -> None:
    """Print per-chunk relevancy breakdown from metric.verdicts_list."""
    for chunk_idx, ((chunk, dist), chunk_verdicts) in enumerate(
        zip(chunks, verdicts_list), start=1
    ):
        verdicts = chunk_verdicts.verdicts
        relevant = sum(1 for v in verdicts if v.verdict.lower() == "yes")
        total = len(verdicts)
        chunk_score = relevant / total if total else 0.0
        dist_str = f"{dist:.4f}" if dist is not None else "?"
        print(
            f"    Chunk {chunk_idx:02d}  [{relevant}/{total} relevant, score={chunk_score:.2f}]  {chunk.book_metadata.title}  |  id={chunk.doc_id}  |  dist={dist_str}"
        )
        for stmt_idx, v in enumerate(verdicts, start=1):
            is_relevant = v.verdict.lower() == "yes"
            label = "RELEVANT  " if is_relevant else "IRRELEVANT"
            print(f"      stmt {stmt_idx:02d} [{label}]  {v.statement}")
            if v.reason and not is_relevant:
                print(f"               reason: {v.reason}")
        print()


def search_and_evaluate(
    *,
    index_name: str,
    query: str,
    top_k: int,
    threshold: float,
    judge_model: str,
) -> tuple[EvalResult, list, list]:
    """Run search query on index, build "test case" from retrieved chunks, then
    evaluate test case with the metric, return evaluation of the search query
    """

    from deepeval.metrics import ContextualRelevancyMetric
    from deepeval.test_case import LLMTestCase

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

    judge = build_judge(judge_model)

    test_case = LLMTestCase(
        input=query,  # ALT: use the full update_chat() function to generate TP queries via search tools
        retrieval_context=[chunk.text.strip() for chunk, _ in search_results],
    )

    # Score = Number of Relevant Statements / Total Number of Statements
    # This the fact that this metric uses "statements" suggests it is designed \
    # more for fact based retrieval than more generally applicable "relevance" \
    # (genre, theme, style, topic)
    metric = ContextualRelevancyMetric(
        threshold=threshold,
        model=judge,
        include_reason=True,
        async_mode=False,
        verbose_mode=False,  # prints stmt verdicts, but unneeded bc we manually extract and print them
    )
    metric.measure(test_case)
    row = EvalResult(
        index_name=index_name,
        query=query,
        score=metric.score,
        passed=bool(metric.success),
        reason=metric.reason,
        retrieved_count=len(search_results),
        top_k=top_k,
        mean_distance=mean_dist,
        min_distance=min_dist,
        max_distance=max_dist,
    )
    return row, search_results, metric.verdicts_list


def print_index_aggregation(rows: list[EvalResult]) -> None:
    """Print a summary that aggregates scores over all queries for each index"""
    if not rows:
        print("No evaluation rows.")
        return

    print("\nIndex level aggregates:")
    by_index: dict[str, list[EvalResult]] = {}
    for row in rows:
        by_index.setdefault(row.index_name, []).append(row)

    for index_name in sorted(by_index):
        group = by_index[index_name]
        scores = [r.score for r in group]
        print(
            f"- {index_name}: mean_score={statistics.mean(scores):.4f} "
            f"median_score={statistics.median(scores):.4f} "
            f"n={len(group)}"
        )


def write_results(
    rows: list[EvalResult],
    out_path: Path,
    indices: list[str],
    queries: list[str],
    args: argparse.Namespace,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "indices": indices,
        "queries": queries,
        "config": {
            "top_k": args.top_k,
            "threshold": args.threshold,
            "judge_model": args.judge_model,
        },
        "rows": [r.__dict__ for r in rows],
    }
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote detailed results to: {out_path}")


def main() -> None:
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

    # Run search queries, evaluate results, display metrics
    case_results: list[EvalResult] = []
    for index_name in indices:
        print(f"\n{'═' * 70}")
        print(f"  INDEX: {index_name}")
        print(f"{'═' * 70}")
        for query in queries:
            print(f"\n  QUERY: {query!r}")

            eval_result, chunks, statement_verdicts = search_and_evaluate(
                index_name=index_name,
                query=query,
                top_k=args.top_k,
                threshold=args.threshold,
                judge_model=args.judge_model,
            )
            case_results.append(eval_result)

            # Print test case evaluation: search results = index + query
            # ALT: instead print with builtins: DisplayConfig(print_results=True) + evaluate(test_case, metric)
            status = "PASS" if eval_result.passed else "FAIL"
            print(
                f"  [{status}] score={eval_result.score:.4f}  retrieved={eval_result.retrieved_count}/{eval_result.top_k}"
            )
            if eval_result.reason:
                print(f"  reason: {eval_result.reason}")
            if not args.suppress_stmts:
                print()
                print_stmt_verdicts(chunks, statement_verdicts)

    print_index_aggregation(case_results)
    write_results(case_results, out_path, indices, queries, args)


if __name__ == "__main__":
    main()
