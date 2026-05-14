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
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path if running directly.
if __name__ == "__main__":
    from dotenv import find_dotenv

    project_root = Path(
        find_dotenv("requirements.txt", raise_error_if_not_found=True)
    ).parent
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
        default=0.0,
        help="DeepEval contextual relevancy pass threshold (default: 0.0, effectively disabled).",
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
        if not args.queries_file.exists():
            raise FileNotFoundError(f"queries file not found: {args.queries_file}")
        for line in args.queries_file.read_text().splitlines():
            line = line.strip()
            if line:
                queries.append(line)
    if not queries:
        queries = STARTER_QUERIES.copy()
    return queries


def build_test_case(
    *,
    index_name: str,
    query: str,
    top_k: int,
) -> "LLMTestCase":
    """Run search query on index, build and return a named LLMTestCase."""
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

    # Score = Number of Relevant Statements / Total Number of Statements.
    # The use of "statements" suggests this metric is designed more for fact-based
    # retrieval than more generally applicable "relevance" (genre, theme, style, topic).
    return LLMTestCase(
        name=f"{index_name} | {query[:50]}",
        input=query,  # ALT: use update_chat() to generate TP queries via search tools
        retrieval_context=[chunk.text.strip() for chunk, _ in search_results],
    )


def print_index_aggregation(evaluation_result) -> None:
    """Print a summary aggregating scores over all queries for each index."""
    if not evaluation_result.test_results:
        print("No evaluation results.")
        return

    print("\nIndex level aggregates:")
    by_index: dict[str, list[float]] = {}
    for test_result in evaluation_result.test_results:
        index_name = test_result.name.split(" | ")[0]
        if test_result.metrics_data:
            score = test_result.metrics_data[0].score
            if score is not None:
                by_index.setdefault(index_name, []).append(score)

    for index_name in sorted(by_index):
        scores = by_index[index_name]
        print(
            f"- {index_name}: mean_score={statistics.mean(scores):.4f} "
            f"median_score={statistics.median(scores):.4f} "
            f"n={len(scores)}"
        )


def write_results(
    evaluation_result,
    out_path: Path,
    indices: list[str],
    queries: list[str],
    args: argparse.Namespace,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for tr in evaluation_result.test_results:
        index_name = tr.name.split(" | ")[0]
        for md in tr.metrics_data:
            rows.append(
                {
                    "name": tr.name,
                    "index_name": index_name,
                    "metric": md.name,
                    "score": md.score,
                    "passed": md.success,
                    "reason": md.reason,
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
        "rows": rows,
    }
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote detailed results to: {out_path}")


def main() -> None:
    from deepeval import evaluate  # type: ignore[import-not-found]
    from deepeval.evaluate.configs import DisplayConfig  # type: ignore[import-not-found]
    from deepeval.metrics import ContextualRelevancyMetric  # type: ignore[import-not-found]

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
    metric = ContextualRelevancyMetric(
        threshold=args.threshold,
        model=judge,
        include_reason=True,
        async_mode=False,
        verbose_mode=True,
    )

    evaluation_result = evaluate(
        test_cases,
        [metric],
        display_config=DisplayConfig(print_results=True, verbose_mode=True),
    )

    print_index_aggregation(evaluation_result)
    write_results(evaluation_result, out_path, indices, queries, args)


if __name__ == "__main__":
    main()
