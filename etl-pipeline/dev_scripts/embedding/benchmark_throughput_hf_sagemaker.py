"""
Benchmark throughput of the SageMaker deployment of HF TEI container.
"""

import asyncio
import dataclasses
import json
import time
from datetime import datetime
from pathlib import Path
from typing import List

import boto3
import numpy as np
import sagemaker

# from sagemaker.predictor import Predictor # requires manual passing of JSONSerializer
# from sagemaker.deserializers import JSONDeserializer
# from sagemaker.serializers import JSONSerializer
from sagemaker.huggingface import HuggingFacePredictor
from transformers import AutoTokenizer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENDPOINT_NAME = "hf-tei-20260423-142720"  # Qwen, g6e
AWS_PROFILE = "sandbox"

TOTAL_REQUESTS = 200  # requests at each concurrency level
# CONCURRENCY_LEVELS = [1, 16, 32, 64]
CONCURRENCY_LEVELS = [1, 4, 8, 10, 13, 16]

# TODO: for higher thruput. try  {inputs: [<str>, <str>]} (this works!),  batch_transform, async inference endpoint

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ~730 tokens of representative research-book text.
# At ~4 chars/token this comes to roughly 2 900 characters.
DUMMY_TEXT = (
    "The history of the printed book stretches back to the fifteenth century, "
    "when Johannes Gutenberg's invention of movable type transformed the "
    "reproduction of knowledge across Europe. Before that pivotal moment, "
    "manuscripts were copied by hand in scriptoria, making books scarce, "
    "expensive, and accessible mainly to clergy and nobility. The press "
    "democratized reading, catalyzed the Protestant Reformation, accelerated "
    "scientific exchange, and ultimately reshaped every domain of human "
    "endeavor. Scholars have traced how the standardization of spelling, "
    "punctuation, and page layout that print imposed gradually altered "
    "cognition itself, fostering the linear, sequential modes of thought that "
    "characterize the modern Western mind. "
    "By the nineteenth century, industrialization brought steam-powered "
    "presses, cheaper paper, and mass literacy campaigns, swelling the "
    "volume of published works to a scale that individual readers could no "
    "longer survey unaided. Libraries responded with classification systems—"
    "Dewey Decimal, Library of Congress—that imposed hierarchical order on "
    "ever-expanding collections. Bibliographers developed detailed catalogs "
    "while scholars theorized about the relationship between text, edition, "
    "and copy, laying the groundwork for modern bibliography and textual "
    "criticism. The concept of the 'definitive edition' emerged alongside "
    "romantic notions of authorial intention, shaping editorial practice for "
    "more than a century. "
    "The digital revolution of the late twentieth century again disrupted "
    "every assumption about how texts are produced, stored, discovered, and "
    "consumed. Optical character recognition made it possible to convert "
    "millions of scanned pages into searchable text, while the World Wide Web "
    "provided a distribution network of unprecedented reach. Projects such as "
    "Project Gutenberg, the Internet Archive, and Google Books brought large "
    "swaths of the public-domain corpus online, although questions of access, "
    "copyright, and quality control complicated each initiative. Digital "
    "humanities scholars began applying quantitative methods—topic modeling, "
    "named-entity recognition, stylometric analysis—to corpora too large for "
    "any single reader, opening new avenues of inquiry while raising debates "
    "about the epistemological status of distant reading. "
    "Embedding models represent the latest turn in this long arc. By mapping "
    "words, sentences, and entire documents into dense numerical vectors, "
    "they capture semantic relationships that keyword search cannot express. "
    "A query about 'cardiac arrest' can now surface documents that discuss "
    "'myocardial infarction' without any lexical overlap, because both phrases "
    "occupy nearby regions of the learned vector space. Retrieval-augmented "
    "generation systems exploit this property to ground large language models "
    "in factual, up-to-date knowledge, reducing hallucination and enabling "
    "citation. For digital research libraries, embedding-based search "
    "promises to make rare and specialized collections discoverable to "
    "scholars who lack the domain vocabulary to formulate precise keyword "
    "queries, lowering barriers and broadening participation in humanistic "
    "research. Evaluating the throughput and latency characteristics of "
    "embedding endpoints is therefore a practical necessity before any "
    "production deployment. "
    "Beyond raw performance, the choice of embedding model carries "
    "significant implications for the quality of downstream retrieval. "
    "Models trained on general web corpora may underperform on specialized "
    "academic or archival text, where domain-specific terminology, archaic "
    "spelling variants, and multilingual content are common. Fine-tuning on "
    "curated library metadata and full-text corpora can close that gap, but "
    "requires careful construction of contrastive training pairs and "
    "rigorous evaluation against human-judged relevance assessments. "
    "Multilingual models add another dimension of complexity: aligning vector "
    "spaces across languages so that a query in English can retrieve a "
    "relevant passage in French or Spanish demands cross-lingual training "
    "objectives and balanced multilingual data. The interplay between model "
    "size, embedding dimensionality, quantization, and serving hardware "
    "determines the Pareto frontier between cost and quality that each "
    "deployment must navigate. Systematic benchmarking across concurrency "
    "levels, batch sizes, and payload lengths is therefore an essential first "
    "step before committing to any particular model, quantization scheme, or "
    "hardware configuration in a production research-library environment."
)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class BatchResult:
    """Aggregated latency and throughput metrics for one concurrency level."""

    concurrency: int
    total_requests: int
    total_time_s: float
    req_per_s: float
    tokens_per_s: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_count: int

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


# ---------------------------------------------------------------------------
# Throughput Benchmark logic
# ---------------------------------------------------------------------------


async def _call_endpoint(predictor: HuggingFacePredictor, semaphore: asyncio.Semaphore):
    """Run one synchronous predictor.predict() call inside a thread."""
    async with semaphore:
        start = time.perf_counter()
        try:
            await asyncio.to_thread(predictor.predict, {"inputs": DUMMY_TEXT})
            elapsed = time.perf_counter() - start
            return elapsed, None
        except Exception as exc:
            elapsed = time.perf_counter() - start
            return elapsed, exc


async def run_benchmark(
    predictor: HuggingFacePredictor,
    concurrency: int,
    total_requests: int,
    token_count: int,
) -> BatchResult:
    """
    Send *total_requests* embedding requests capped at *concurrency* in-flight at once.
    Return summary of throughput.
    """

    semaphore = asyncio.Semaphore(concurrency)
    tasks = [_call_endpoint(predictor, semaphore) for _ in range(total_requests)]

    # Run inference for *total_requests*
    print(f"  Benchmarking concurrency={concurrency} ...")
    wall_start = time.perf_counter()
    outcomes = await asyncio.gather(*tasks)
    wall_time = time.perf_counter() - wall_start

    # Collect latency on success, record error on failure
    latencies = []
    errors = 0
    for elapsed, exc in outcomes:
        if exc is not None:
            errors += 1
            print(f"    Request error: {exc}")
        else:
            latencies.append(elapsed)

    latencies_ms = np.array(latencies) * 1000

    result = BatchResult(
        concurrency=concurrency,
        total_requests=total_requests,
        total_time_s=wall_time,
        req_per_s=total_requests / wall_time,
        tokens_per_s=(total_requests * token_count) / wall_time,
        avg_latency_ms=float(np.mean(latencies_ms)) if latencies_ms.size else 0.0,
        p50_latency_ms=float(np.percentile(latencies_ms, 50))
        if latencies_ms.size
        else 0.0,
        p95_latency_ms=float(np.percentile(latencies_ms, 95))
        if latencies_ms.size
        else 0.0,
        p99_latency_ms=float(np.percentile(latencies_ms, 99))
        if latencies_ms.size
        else 0.0,
        error_count=errors,
    )

    # Print batch results
    print(
        f"    concurrency={concurrency:>2}  "
        f"throughput={result.req_per_s:.2f} req/s  "
        f"avg_latency={result.avg_latency_ms:.1f} ms  "
        f"p95={result.p95_latency_ms:.1f} ms  "
        f"errors={errors}"
    )
    return result


# ---------------------------------------------------------------------------
# Entry point - Benchmark Multiple Concurrencies
# ---------------------------------------------------------------------------


def get_endpoint_info(predictor: HuggingFacePredictor) -> tuple[str | None, str | None]:
    """Return (HF_MODEL_ID, instance_type) by introspecting the predictor's session.

    Uses predictor._get_endpoint_config_name() (caches describe_endpoint) so we only
    need two API calls: describe_endpoint_config + describe_model.
    """
    sm = predictor.sagemaker_session.sagemaker_client
    config = sm.describe_endpoint_config(
        EndpointConfigName=predictor._get_endpoint_config_name()
    )
    variant = config["ProductionVariants"][0]
    instance_type = variant.get("InstanceType")
    model = sm.describe_model(ModelName=variant["ModelName"])
    hf_model_id = model["PrimaryContainer"].get("Environment", {}).get("HF_MODEL_ID")
    return hf_model_id, instance_type


async def main() -> List[BatchResult]:
    """Run the benchmark across all CONCURRENCY_LEVELS and write results to OUTPUT_FILE."""
    boto3.setup_default_session(profile_name=AWS_PROFILE)
    session = sagemaker.Session()

    predictor = HuggingFacePredictor(
        endpoint_name=ENDPOINT_NAME,
        sagemaker_session=session,
    )

    hf_model_id, instance_type = get_endpoint_info(predictor)

    if hf_model_id is None:
        raise ValueError(
            "Could not determine HF_MODEL_ID from endpoint config; cannot load tokenizer."
        )
    _tokenizer = AutoTokenizer.from_pretrained(hf_model_id)
    dummy_text_tokens: int = len(_tokenizer.encode(DUMMY_TEXT))
    # ALT: use the /tokenize endpoint of the deployed model. see: https://huggingface.github.io/text-embeddings-inference/#/Text%20Embeddings%20Inference/tokenize

    print(f"Endpoint      : {ENDPOINT_NAME}")
    print(f"Instance type : {instance_type or '(not found)'}")
    print(f"HF_MODEL_ID   : {hf_model_id}")
    print(f"Requests      : {TOTAL_REQUESTS} per concurrency level")
    print(f"Dummy text    : ~{dummy_text_tokens} tokens\n")

    assert max(CONCURRENCY_LEVELS) < TOTAL_REQUESTS, (
        f"Max concurrency ({max(CONCURRENCY_LEVELS)}) must be less than TOTAL_REQUESTS ({TOTAL_REQUESTS})"
    )

    # Run inference throughput benchmark for all concurrency levels
    results: List[BatchResult] = []
    for concurrency in CONCURRENCY_LEVELS:
        result = await run_benchmark(
            predictor, concurrency, TOTAL_REQUESTS, dummy_text_tokens
        )
        results.append(result)

    # Print multi-concurrency benchmark summary table
    print("\n--- Summary ---")
    header = f"{'Concurrency':>11}  {'Req/s':>8}  {'Tok/s':>10}  {'Avg (ms)':>10}  {'P50 (ms)':>10}  {'P95 (ms)':>10}  {'P99 (ms)':>10}  {'Errors':>7}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.concurrency:>11}  "
            f"{r.req_per_s:>8.2f}  "
            f"{r.tokens_per_s:>10.1f}  "
            f"{r.avg_latency_ms:>10.1f}  "
            f"{r.p50_latency_ms:>10.1f}  "
            f"{r.p95_latency_ms:>10.1f}  "
            f"{r.p99_latency_ms:>10.1f}  "
            f"{r.error_count:>7}"
        )

    # Serialize results to JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = Path(__file__).parent / "results" / f"benchmark_{timestamp}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "endpoint": ENDPOINT_NAME,
        "instance_type": instance_type,
        "hf_model_id": hf_model_id,
        "results": [r.to_dict() for r in results],
    }
    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to {output_file}")

    return results


if __name__ == "__main__":
    asyncio.run(main())
