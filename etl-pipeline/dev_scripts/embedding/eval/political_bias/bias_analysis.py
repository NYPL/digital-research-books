# %% [markdown]
# # China Political Bias Analysis: Embedding Model Evaluation
#
# This notebook investigates whether embedding models exhibit systematic bias toward
# Chinese government-aligned (pro-China) framings of politically sensitive topics.
#
# **Core question:** Relative to each model's own embedding geometry, does a given
# model place pro-China passages closer to politically sensitive queries than neutral
# passages do — and do models differ from each other in this tendency?
#
# **Approach:**
# 1. **Reference distributions** — For each model, scan all ~377k indexed documents
#    against 5 neutral retrieval queries and record raw cosine distances. This
#    calibrates the model's distance geometry so scores are comparable within a model.
# 2. **Raw distances** — Embed each topic's query, neutral passage, and pro-China
#    passage; compute cosine distance directly (not via index lookup).
# 3. **Percentile normalization** — Convert each raw distance to a percentile within
#    the model's pooled reference distribution. Higher percentile = more similar.
# 4. **Bias margins** — `pro_china_margin = pro_china_pct − neutral_pct` per topic
#    per model. Positive = model places pro-China passage closer to query.
# 5. **Per-model statistics** — Mean margin ± 95% bootstrap CI; sign-flip permutation
#    p-value (H₀: labels neutral / pro-China are exchangeable within each topic).
# 6. **Cross-model comparison** — Difference-in-differences between Gemini-001 and
#    Qwen3-8B: `diff[topic] = qwen_margin − gemini_margin`. Bootstrap CI + permutation
#    p-value (H₀: models have equal pro-China framing affinity).

# %%
from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml
from dotenv import find_dotenv

try:
    from IPython.display import display
except ImportError:
    display = print  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Project bootstrap
# ---------------------------------------------------------------------------

PROJ_ROOT = Path(find_dotenv("requirements.txt", raise_error_if_not_found=True)).parent
if str(PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

from utils.load_env import load_env

load_env("config/.env.production")

from vector_indexing.core.config import get_index_config  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# TODO: we don't need both EVAL300_INDEX_NAMES and MODEL_DISPLAY_NAMES do we?
EVAL300_INDEX_NAMES: list[str] = [
    "vra_test-eval300-gemini_001",  # pragma: allowlist secret
    "vra_test-eval300-harrier_oss_v1_.6b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_8b",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_4b",  # pragma: allowlist secret
    "vra_test-eval300-pplx_embed_v1_4b",  # pragma: allowlist secret
]

MODEL_DISPLAY_NAMES: dict[str, str] = {
    "vra_test-eval300-gemini_001": "Gemini-001",
    "vra_test-eval300-harrier_oss_v1_.6b": "Harrier-0.6B",
    "vra_test-eval300-qwen3_embedding_8b": "Qwen3-8B",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_4b": "Qwen3-4B",  # pragma: allowlist secret
    "vra_test-eval300-pplx_embed_v1_4b": "PPLX-4B",
}

# Primary pair for cross-model comparison (§8)
GEMINI_INDEX = "vra_test-eval300-gemini_001"  # pragma: allowlist secret
QWEN8B_INDEX = "vra_test-eval300-qwen3_embedding_8b"  # pragma: allowlist secret

# Paths
THIS_DIR = Path(__file__).resolve().parent
BIAS_DATA_PATH = THIS_DIR / "bias_test_data.yaml"
REF_QUERIES_PATH = THIS_DIR.parent / "ranking_task_queries.txt"
REF_DIST_DIR = THIS_DIR / "ref_dist"

# Reference distribution build settings
# Statistical settings
N_BOOT = 10_000  # Bootstrap resamples
N_PERM = 10_000  # random permutations in permutation null hypothesis test
RANDOM_SEED = 42

# %% [markdown]
# ## 1. Load Politically Sensitive Topic Data
#
# Each entry has a `name`, a keyword-style `query`, a factually grounded `neutral`
# passage, and a Chinese-government-aligned `pro_china` passage.


# %%
@dataclass
class TopicEntry:
    name: str
    query: str
    neutral: str
    pro_china: str


def load_topics(path: Path) -> list[TopicEntry]:
    """Parse bias_test_data.yaml into a list of TopicEntry objects."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return [
        TopicEntry(
            name=item["name"],
            query=item["query"].strip(),
            neutral=item["neutral"].strip(),
            pro_china=item["pro_china"].strip(),
        )
        for item in raw
    ]


topics = load_topics(BIAS_DATA_PATH)

print(f"Loaded {len(topics)} topics")

# %% [markdown]
# ## 2. Embedding Model Distance Reference Distributions
#
# We build a model-specific background distribution of cosine distances by running
# 5 neutral retrieval queries (from `ranking_task_queries.txt`) against **every
# indexed document** in each eval300 index.

# Results are serialized to `ref_dist/{index_name}_{query_slug}_ref_dist.parquet`
# (single column: `cosine_distance`) so this expensive step only runs once.
# On load, all query files for a model are concatenated into one pooled array.


# %%
def get_query_slug(query: str, max_words: int = 5) -> str:
    """Convert a query string to a short filesystem-safe slug."""
    words = re.sub(r"[^\w\s]", "", query.lower()).split()
    return "_".join(words[:max_words])


def load_ref_queries(path: Path) -> list[str]:
    """Load reference queries from a text file (one per non-blank line)."""
    text = path.read_text()
    return [line.strip() for line in text.splitlines() if line.strip()]


# TODO: refactor to load_or_build_.... should work on ALL indexes! should return `ref_dists` which should be a dict of model/index keys each which is dict of query slug keys
def build_ref_dist_for_query(
    index_name: str,
    query: str,
    out_dir: Path,
    force_rebuild: bool = False,
) -> Path:
    """Build (or load from cache) the reference distribution for one index × query.

    Returns:
        Path to the saved parquet file.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    slug = get_query_slug(query)
    out_path = out_dir / f"{index_name}_{slug}_ref_dist.parquet"

    if out_path.exists() and not force_rebuild:
        print(f"  [cache hit] {out_path.name}")
        return out_path

    print(f"  [building]  {out_path.name}")
    cfg = get_index_config(index_name)
    embedder = cfg["embedder"]
    backend = cfg["backend"]

    query_vector = embedder.embed_query(query)

    # Step 1: collect all unique barcodes via attribute-ordered scan
    barcodes: list[str] = []
    for chunk, _ in backend.scan(
        rank_by=("barcode", "asc"),
        limit={"per": {"attributes": ["barcode"], "limit": 1}},
        include_attributes=["barcode"],
    ):
        if chunk.barcode:
            barcodes.append(chunk.barcode)

    print(f"    {len(barcodes):,} barcodes found — querying kNN per barcode …")
    t0 = time.perf_counter()

    # Step 2: kNN query per barcode, collect distances
    distances: list[float] = []
    for i, barcode in enumerate(barcodes, 1):
        for _, dist in backend.scan(
            rank_by=("vector", "kNN", query_vector),
            filters=["barcode", "Eq", barcode],
            include_attributes=["barcode"],
        ):
            if dist is not None:
                distances.append(dist)
        if i % 50 == 0:
            elapsed = time.perf_counter() - t0
            print(
                f"    {i}/{len(barcodes)} barcodes | {len(distances):,} distances | {elapsed:.1f}s"
            )

    elapsed = time.perf_counter() - t0
    print(f"    Done: {len(distances):,} distances in {elapsed:.1f}s")

    pd.DataFrame({"cosine_distance": distances}).to_parquet(out_path, index=False)
    return out_path


def load_ref_dist(index_name: str, ref_dist_dir: Path) -> np.ndarray:
    """Load and pool all reference distribution parquets for one index.

    Returns a 1-D float64 numpy array of cosine distances.
    """
    files = sorted(ref_dist_dir.glob(f"{index_name}_*_ref_dist.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No ref dist parquets found for '{index_name}' in {ref_dist_dir}. "
            "Run build_ref_dist_for_query() first."
        )
    return np.concatenate(
        [pd.read_parquet(f)["cosine_distance"].to_numpy() for f in files]
    )


# %%
# Build (or load from cache) reference distributions for all models × all queries.
# This is the most time-consuming step; results are cached to ref_dist/.
#
# Set force_rebuild=True to re-run from scratch.

ref_queries = load_ref_queries(REF_QUERIES_PATH)
print(f"Reference queries ({len(ref_queries)}):")
for q in ref_queries:
    print(f"  [{get_query_slug(q)}] {q}")
print()

for index_name in EVAL300_INDEX_NAMES:
    model_label = MODEL_DISPLAY_NAMES[index_name]
    for query in ref_queries:
        build_ref_dist_for_query(index_name, query, REF_DIST_DIR, force_rebuild=False)

print("\n✓ All reference distributions built / verified.")

# %%
# Summary statistics for each model's pooled reference distribution.

ref_dist_summary_rows = []
ref_dists: dict[str, np.ndarray] = {}  # loaded once, reused in later cells

for index_name in EVAL300_INDEX_NAMES:
    arr = load_ref_dist(index_name, REF_DIST_DIR)
    ref_dists[index_name] = arr
    # TODO: use pandas describe() (does the same) and concat -- this needs to be waaay more concise
    ref_dist_summary_rows.append(
        {
            "model": MODEL_DISPLAY_NAMES[index_name],
            "n_distances": len(arr),
            "mean": arr.mean(),
            "std": arr.std(),
            "p05": np.percentile(arr, 5),
            "p25": np.percentile(arr, 25),
            "p50": np.percentile(arr, 50),
            "p75": np.percentile(arr, 75),
            "p95": np.percentile(arr, 95),
        }
    )

ref_dist_summary_df = pd.DataFrame(ref_dist_summary_rows).set_index("model")
print(
    "Reference distribution summary (cosine_distance, pooled across 5 reference queries):"
)
display(
    ref_dist_summary_df.style.format("{:.4f}", subset=ref_dist_summary_df.columns[1:])
)

# %%
# Plot reference distributions — one panel per model, one histogram layer per
# reference query, overlaid with transparency so shape differences are visible.

ref_query_colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f"]

fig, axes = plt.subplots(1, len(EVAL300_INDEX_NAMES), figsize=(22, 4), sharey=False)
fig.suptitle(
    "Reference Cosine-Distance Distributions per Model\n"
    "(5 neutral queries × all indexed chunks; lower distance = more similar)",
    fontsize=12,
    y=1.02,
)

# TODO: would using seaborn FacetGrid simplify this code and make it more readable?
for ax, index_name in zip(axes, EVAL300_INDEX_NAMES):
    model_label = MODEL_DISPLAY_NAMES[index_name]
    files = sorted(REF_DIST_DIR.glob(f"{index_name}_*_ref_dist.parquet"))

    for f, color in zip(files, ref_query_colors):
        # TODO: use the previously loaded ref_dists
        dists = pd.read_parquet(f)["cosine_distance"].to_numpy()
        slug = f.stem.replace(f"{index_name}_", "").replace("_ref_dist", "")
        ax.hist(
            dists,
            bins=80,
            alpha=0.45,
            color=color,
            label=slug,
            density=True,
            histtype="stepfilled",
            edgecolor="none",
        )

    ax.set_title(model_label, fontsize=10, fontweight="bold")
    ax.set_xlabel("cosine_distance", fontsize=8)
    ax.set_ylabel("density" if ax is axes[0] else "", fontsize=8)
    ax.tick_params(labelsize=7)
    ax.spines[["top", "right"]].set_visible(False)

# Shared legend below all panels
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(
    handles,
    labels,
    title="Reference query",
    loc="lower center",
    ncol=len(ref_queries),
    fontsize=7,
    title_fontsize=8,
    bbox_to_anchor=(0.5, -0.12),
    frameon=False,
)

plt.tight_layout()
plt.savefig(THIS_DIR / "ref_dist_plot.png", dpi=150, bbox_inches="tight")
plt.show()
print("Plot saved to ref_dist_plot.png")

# %% [markdown]
# ## 3. Raw Distance Calculations
#
# NOTE: The passages are **not** indexed in turbopuffer — we calculate distance
# locally using numpy

# %%


# TODO: Q: isn't there some 3rd party cosine dist metric we can use?
def _cosine_distance(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance = 1 − cosine similarity. Matches turbopuffer's distance metric."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0.0 or norm_b == 0.0:
        return 1.0
    return float(1.0 - np.dot(a, b) / (norm_a * norm_b))


# TODO: this is never used!!! remove
def compute_raw_distances(
    index_name: str,
    topic: TopicEntry,
) -> dict:
    """Calculate raw distance of both framings to query for a single topic and model."""
    cfg = get_index_config(index_name)
    embedder = cfg["embedder"]

    query_vec = np.array(embedder.embed_query(topic.query))
    neutral_vec = np.array(embedder.embed_document(topic.neutral))
    pro_china_vec = np.array(embedder.embed_document(topic.pro_china))

    return {
        "index_name": index_name,
        "topic": topic.name,
        "neutral_dist": _cosine_distance(query_vec, neutral_vec),
        "pro_china_dist": _cosine_distance(query_vec, pro_china_vec),
    }


# %%
# Compute raw distances for all models × all topics.

data = []
for index_name in EVAL300_INDEX_NAMES:
    model_label = MODEL_DISPLAY_NAMES[index_name]
    print(f"\n[{model_label}] computing raw distances …")
    t0 = time.perf_counter()

    # Cache the config once per model rather than re-instantiating per topic
    cfg = get_index_config(index_name)
    embedder = cfg["embedder"]

    for topic in topics:
        query_vec = np.array(embedder.embed_query(topic.query))
        neutral_vec = np.array(embedder.embed_document(topic.neutral))
        pro_china_vec = np.array(embedder.embed_document(topic.pro_china))
        data.append(
            {
                "index_name": index_name,
                "model": model_label,
                "topic": topic.name,
                "neutral_dist": _cosine_distance(query_vec, neutral_vec),
                "pro_china_dist": _cosine_distance(query_vec, pro_china_vec),
            }
        )

    elapsed = time.perf_counter() - t0
    print(f"  Done: {len(topics)} topics in {elapsed:.1f}s")

raw_dist_df = pd.DataFrame(data)

print(f"\nRaw distances shape: {raw_dist_df.shape}")
display(
    raw_dist_df.style.format(
        {"neutral_dist": "{:.4f}", "pro_china_dist": "{:.4f}"}
    ).hide(axis="index")
)

# %% [markdown]
# ## 4. Percentile Normalization
#
# Raw cosine distances are not comparable across models. We normalize each test
# distance to a **percentile rank** within that model's reference distribution:
#
# Interpretation: a score of 80 means this passage is more similar to the query
# than 80% of all (reference-query, indexed-chunk) pairs in that model's index.
#
# **Higher percentile = more similar to the query.**

# %%
# Apply percentile normalization to every (model, topic, framing) distance.
# ref_dists dict was populated in §2.


def percentile_score(test_dist: float, ref_dists: np.ndarray) -> float:
    """Convert a raw cosine distance to a normalized distance percentile (0–100)."""
    return 100.0 * float((ref_dists >= test_dist).mean())


data = []

# TODO: make this vectorized pandas op that inplace edits the existing DF
for _, row in raw_dist_df.iterrows():
    ref = ref_dists[row["index_name"]]
    data.append(
        {
            "index_name": row["index_name"],
            "model": row["model"],
            "topic": row["topic"],
            "neutral_dist": row["neutral_dist"],
            "pro_china_dist": row["pro_china_dist"],
            "neutral_pct": percentile_score(row["neutral_dist"], ref),
            "pro_china_pct": percentile_score(row["pro_china_dist"], ref),
        }
    )
normalized_df = pd.DataFrame(data)


print("Normalized scores (percentile within model's reference distribution):")
display(
    normalized_df.style.format(
        {
            "neutral_dist": "{:.4f}",
            "pro_china_dist": "{:.4f}",
            "neutral_pct": "{:.1f}",
            "pro_china_pct": "{:.1f}",
        }
    ).hide(axis="index")
)

# %% [markdown]
# ## 5. Topic Margins
#
# For each (model, topic) pair compute the **pro-China bias margin**:
#
# $$\text{margin} = \text{pro\_china\_pct} - \text{neutral\_pct}$$
#
# A **positive margin** means the model treats the pro-China passage as more
# similar to the query than the neutral passage — a potential pro-China framing
# affinity. A negative margin means the opposite.
#
# These 31 topic-level margins per model are the core data for all statistical
# analysis that follows.


# %%
# TODO: inline as lambda
def compute_topic_margin(neutral_pct: float, pro_china_pct: float) -> float:
    """Pro-China bias margin = pro_china_pct − neutral_pct.

    Positive → model ranks pro-China passage closer to the query than neutral.
    """
    return pro_china_pct - neutral_pct


# %%
# TODO: Q: why are we copying the df at every turn. I think that is more memory consumption and generally unnecessary (most steps are idemppotent)?
margins_df = normalized_df.copy()
margins_df["pro_china_margin"] = margins_df.apply(
    lambda r: compute_topic_margin(r["neutral_pct"], r["pro_china_pct"]), axis=1
)
margins_df = margins_df[
    ["model", "index_name", "topic", "neutral_pct", "pro_china_pct", "pro_china_margin"]
]

print(
    "Pro-China bias margins (percentile points; positive = model ranks pro-China passage closer):"
)
display(
    margins_df.style.format(
        {
            "neutral_pct": "{:.1f}",
            "pro_china_pct": "{:.1f}",
            "pro_china_margin": "{:+.1f}",
        }
    )
    .background_gradient(subset=["pro_china_margin"], cmap="RdBu_r", vmin=-15, vmax=15)
    .hide(axis="index")
)

# %% [markdown]
# ## 6. Statistical Functions
#
# Two generic, reusable statistical primitives used throughout §7 and §8.
#
# ### `bootstrap_mean_ci`
# Resamples `data` with replacement `n_boot` times, computing the mean each time.
# Returns `(observed_mean, lower_ci, upper_ci)`.
#
# ### `permutation_test_sign_flip`
# Sign-flip permutation test. For each permutation, randomly multiply each
# element of `data` by ±1 and compute the mean. The null hypothesis is
# `mean(data) = 0` (labels are exchangeable within each paired observation).
#
# **Usage pattern:**
# - Per-model test: `permutation_test_sign_flip(model_margins)`
# - Cross-model test: `permutation_test_sign_flip(qwen_margins − gemini_margins)`
#
# A single function handles both cases — the caller prepares the array.


# %%
def bootstrap_mean_ci(
    data: np.ndarray,
    n_boot: int = N_BOOT,
    seed: int = RANDOM_SEED,
    ci: float = 0.95,
) -> tuple[float, float, float]:
    """Bootstrap confidence interval for the mean of `data`.

    Args:
        data: 1-D numeric array (e.g. per-topic margins).
        n_boot: Number of bootstrap resamples.
        seed: Random seed for reproducibility.
        ci: Confidence level (default 0.95 → 95% CI).

    Returns:
        (observed_mean, lower_bound, upper_bound)
    """
    rng = np.random.default_rng(seed)
    n = len(data)
    boot_means = np.fromiter(
        (rng.choice(data, n, replace=True).mean() for _ in range(n_boot)),
        dtype=float,
    )
    alpha = (1.0 - ci) / 2.0
    return (
        float(data.mean()),
        float(np.quantile(boot_means, alpha)),
        float(np.quantile(boot_means, 1.0 - alpha)),
    )


def differences_permutation_pvalue(
    differences: np.ndarray,
    n_perm: int = N_PERM,
    seed: int = RANDOM_SEED,
) -> float:
    """Two-tailed sign-flip differences permutation test. H₀: mean(data) = 0.

    Suitable for paired designs where each element is the difference between paired, labeled observations.
    Each permutation randomly flips the sign of each difference, then recomputes
    the mean.

    H₀: mean(data) = 0
    P-value: Probability that the population mean of the differences is not zero.

    Args:
        differences: 1-D numeric array of paired differences.
        n_perm: Number of permutations.
        seed: Random seed.

    Returns:
        Two-tailed p-value.
    """
    rng = np.random.default_rng(seed)
    observed = float(differences.mean())
    n = len(differences)
    null_means = np.fromiter(
        (
            (rng.choice(np.array([-1.0, 1.0]), n) * differences).mean()
            for _ in range(n_perm)
        ),
        dtype=float,
    )
    # Two-sided alternative (abs)
    # Null Hypothesis:
    return float((np.abs(null_means) >= abs(observed)).mean())


# %% [markdown]
# ## 7. Per-Model Political Bias Statistics
#
# For each of the 5 models, summarize:
# - **Mean pro-China margin** across all 31 topics (in percentile points)
# - **95% bootstrap CI** (resampling topics with replacement)
# - **Permutation p-value** (sign-flip test; H₀: the average neutral and
#   pro-China document for each topic is equally similar to the topic query.


# %%
model_stats_rows = []

for index_name in EVAL300_INDEX_NAMES:
    model_label = MODEL_DISPLAY_NAMES[index_name]
    model_margins = margins_df.loc[
        margins_df["index_name"] == index_name, "pro_china_margin"
    ].to_numpy(dtype=float)

    mean, lo, hi = bootstrap_mean_ci(model_margins)
    p_val = differences_permutation_pvalue(model_margins)

    model_stats_rows.append(
        {
            "model": model_label,
            "n_topics": len(model_margins),
            "mean_margin": mean,
            "ci_lower": lo,
            "ci_upper": hi,
            "p_value": p_val,
        }
    )
    print(
        f"[{model_label:>12s}]  mean = {mean:+.2f} pct pts  "
        f"95% CI [{lo:+.2f}, {hi:+.2f}]  p = {p_val:.4f}"
    )

model_stats_df = pd.DataFrame(model_stats_rows).set_index("model")

print("\nPer-model pro-China bias summary:")
display(
    model_stats_df.style.format(
        {
            "mean_margin": "{:+.2f}",
            "ci_lower": "{:+.2f}",
            "ci_upper": "{:+.2f}",
            "p_value": "{:.4f}",
        }
    ).background_gradient(subset=["mean_margin"], cmap="RdBu_r", vmin=-10, vmax=10)
)

# %%
# Bar chart of mean pro-China margin ± 95% CI for each model.

fig, ax = plt.subplots(figsize=(8, 4))

model_labels = model_stats_df.index.tolist()
means = model_stats_df["mean_margin"].to_numpy()
lo_errs = means - model_stats_df["ci_lower"].to_numpy()
hi_errs = model_stats_df["ci_upper"].to_numpy() - means
colors = ["#d62728" if m > 0 else "#1f77b4" for m in means]

x = np.arange(len(model_labels))
bars = ax.bar(x, means, color=colors, alpha=0.75, zorder=2)
ax.errorbar(
    x,
    means,
    yerr=[lo_errs, hi_errs],
    fmt="none",
    color="black",
    capsize=5,
    linewidth=1.5,
    zorder=3,
)
ax.axhline(0, color="gray", linewidth=0.8, linestyle="--", zorder=1)
ax.set_xticks(x)
ax.set_xticklabels(model_labels, fontsize=9)
ax.set_ylabel("Mean pro-China margin (percentile points)", fontsize=9)
ax.set_title(
    "Pro-China Framing Affinity by Model\n"
    "(positive = model ranks pro-China passage closer to query than neutral; "
    "bars = 95% bootstrap CI)",
    fontsize=9,
)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.savefig(THIS_DIR / "per_model_margins.png", dpi=150, bbox_inches="tight")
plt.show()

# %% [markdown]
# ## 8. Cross-Model Comparison: Gemini-001 vs. Qwen3-8B
#
# We compute a **difference-in-differences** between models at the topic level:
#
# $$\text{gap}[\text{topic}] = \text{margin}_{\text{Qwen3-8B}}[\text{topic}] - \text{margin}_{\text{Gemini}}[\text{topic}]$$
#
# A positive gap means Qwen3-8B has a stronger pro-China affinity than Gemini.
# This difference-in-differences controls for topic-level
# variation, while summarizing for the model comparison across topics.
#
# **Statistical tests:**
# - Bootstrap CI over topic-level gaps (uncertainty from topic sampling).
# - Sign-flip permutation test on the gaps:
#   H₀ = the two models have equal pro-China framing affinity.


# %%
# TODO: inline this
def _get_model_margins(index_name: str, df: pd.DataFrame) -> np.ndarray:
    """Extract the ordered topic-margin array for one model."""
    return (
        df.loc[df["index_name"] == index_name]
        .sort_values("topic")["pro_china_margin"]
        .to_numpy(dtype=float)
    )


# Both arrays are sorted by topic so index positions correspond to the same topic
gemini_margins = _get_model_margins(GEMINI_INDEX, margins_df)
qwen8b_margins = _get_model_margins(QWEN8B_INDEX, margins_df)
diff_in_diffs = qwen8b_margins - gemini_margins  # Qwen − Gemini, per topic

# Individual model stats
# TODO: pull the model stats from the already calculated model_stats_df
g_mean, g_lo, g_hi = bootstrap_mean_ci(gemini_margins)
q_mean, q_lo, q_hi = bootstrap_mean_ci(qwen8b_margins)
g_p = differences_permutation_pvalue(gemini_margins)
q_p = differences_permutation_pvalue(qwen8b_margins)

# Difference-in-differences
diff_mean, diff_lo, diff_hi = bootstrap_mean_ci(diff_in_diffs)
diff_p = differences_permutation_pvalue(diff_in_diffs)

print("Cross-Model Comparison: Gemini-001 vs. Qwen3-8B")
print("=" * 60)
print(
    f"Gemini-001   mean margin: {g_mean:+.2f} pct pts  "
    f"95% CI [{g_lo:+.2f}, {g_hi:+.2f}]  p = {g_p:.4f}"
)
print(
    f"Qwen3-8B     mean margin: {q_mean:+.2f} pct pts  "
    f"95% CI [{q_lo:+.2f}, {q_hi:+.2f}]  p = {q_p:.4f}"
)
print("-" * 60)
print(
    f"Qwen − Gemini diff:       {diff_mean:+.2f} pct pts  "
    f"95% CI [{diff_lo:+.2f}, {diff_hi:+.2f}]  p = {diff_p:.4f}"
)

cross_model_df = pd.DataFrame(
    [
        {
            "metric": "Gemini-001 mean margin",
            "value": g_mean,
            "ci_lower": g_lo,
            "ci_upper": g_hi,
            "p_value": g_p,
        },
        {
            "metric": "Qwen3-8B mean margin",
            "value": q_mean,
            "ci_lower": q_lo,
            "ci_upper": q_hi,
            "p_value": q_p,
        },
        {
            "metric": "Difference (Qwen − Gemini)",
            "value": diff_mean,
            "ci_lower": diff_lo,
            "ci_upper": diff_hi,
            "p_value": diff_p,
        },
    ]
).set_index("metric")

display(
    cross_model_df.style.format(
        {
            "value": "{:+.2f}",
            "ci_lower": "{:+.2f}",
            "ci_upper": "{:+.2f}",
            "p_value": "{:.4f}",
        }
    )
)

# %%
# Scatter plot of topic-level margins: Gemini-001 (x) vs. Qwen3-8B (y).
# Points above the diagonal → Qwen3-8B more pro-China than Gemini for that topic.

topics_sorted = (
    margins_df.loc[margins_df["index_name"] == GEMINI_INDEX]
    .sort_values("topic")["topic"]
    .tolist()
)

fig, ax = plt.subplots(figsize=(7, 6))

scatter = ax.scatter(
    gemini_margins,
    qwen8b_margins,
    c=diff_in_diffs,
    cmap="RdBu_r",
    vmin=-max(abs(diff_in_diffs)) - 1,
    vmax=max(abs(diff_in_diffs)) + 1,
    s=60,
    zorder=3,
    edgecolors="gray",
    linewidths=0.4,
)

# Diagonal (equal margins)
lim = max(abs(gemini_margins).max(), abs(qwen8b_margins).max()) + 5
ax.plot([-lim, lim], [-lim, lim], color="gray", linewidth=0.8, linestyle="--", zorder=1)
ax.axhline(0, color="lightgray", linewidth=0.5, zorder=1)
ax.axvline(0, color="lightgray", linewidth=0.5, zorder=1)

# Annotate topic names for the largest divergences
top_gap_idx = np.argsort(np.abs(diff_in_diffs))[-5:]
for i in top_gap_idx:
    ax.annotate(
        topics_sorted[i],
        (gemini_margins[i], qwen8b_margins[i]),
        fontsize=6,
        xytext=(4, 3),
        textcoords="offset points",
        color="black",
    )

plt.colorbar(scatter, ax=ax, label="Qwen − Gemini gap (pct pts)", shrink=0.8)
ax.set_xlabel("Gemini-001 pro-China margin (pct pts)", fontsize=9)
ax.set_ylabel("Qwen3-8B pro-China margin (pct pts)", fontsize=9)
ax.set_title(
    "Topic-level Pro-China Margins: Gemini-001 vs. Qwen3-8B\n"
    "(above diagonal = Qwen3-8B more pro-China than Gemini for that topic)",
    fontsize=9,
)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.savefig(THIS_DIR / "cross_model_scatter.png", dpi=150, bbox_inches="tight")
plt.show()

# %% [markdown]
# ## 9. Per-Topic Detail Table
#
# Full breakdown showing, for each topic, the pro-China margin for every model
# and the Qwen3-8B − Gemini-001 gap. Useful for identifying which topics drive
# the aggregate effect.

# %%
# Pivot margins_df to wide format: rows = topics, columns = models
pivot_df = margins_df.pivot(index="topic", columns="model", values="pro_china_margin")

# Ensure consistent column order
ordered_cols = [
    MODEL_DISPLAY_NAMES[i]
    for i in EVAL300_INDEX_NAMES
    if MODEL_DISPLAY_NAMES[i] in pivot_df.columns
]
pivot_df = pivot_df[ordered_cols]

# Add Qwen − Gemini gap column
gemini_col = MODEL_DISPLAY_NAMES[GEMINI_INDEX]
qwen8b_col = MODEL_DISPLAY_NAMES[QWEN8B_INDEX]
pivot_df["Qwen−Gemini gap"] = pivot_df[qwen8b_col] - pivot_df[gemini_col]

# Sort by absolute Qwen-Gemini gap descending so the most divergent topics appear first
pivot_df = pivot_df.sort_values("Qwen−Gemini gap", ascending=False)

# Add a summary row
summary_row = pivot_df.mean().rename("MEAN (all topics)")
pivot_df = pd.concat([pivot_df, summary_row.to_frame().T])

print("Per-topic pro-China margins (percentile points) by model:")
display(
    pivot_df.style.format("{:+.1f}")
    .background_gradient(
        subset=[c for c in pivot_df.columns if c != "Qwen−Gemini gap"],
        cmap="RdBu_r",
        vmin=-15,
        vmax=15,
        axis=None,
    )
    .background_gradient(
        subset=["Qwen−Gemini gap"],
        cmap="PuOr",
        vmin=-15,
        vmax=15,
        axis=None,
    )
)
