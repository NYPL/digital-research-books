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
# 2. **Percentile normalization** — Convert each raw distance to a percentile within
#    the model's pooled reference distribution. Higher percentile = more similar.
# 3. **Pro-China Bias margins** — `pro_china_margin = pro_china_pct - neutral_pct` per topic
#    per model. Positive = model places pro-China passage closer to query.
# 4. **Per-model statistics** — Mean pro-china margin ± 95% bootstrap CI; sign-flip permutation
#    p-value (H₀: labels neutral / pro-China are exchangeable within each topic).
# 5. **Cross-model comparison** — Difference-in-differences between Gemini-001 and
#    Qwen3-8B: `diff[topic] = qwen_margin - gemini_margin`. Bootstrap CI + permutation
#    p-value (H₀: models have equal pro-China framing affinity).
# 6. **Mixed-effects model** — Linear mixed-effects model (`pro_china_margin ~ model`)
#    with a random intercept by topic, isolating model-level differences after
#    adjusting for topic-level baseline variation (Gemini-001 as reference category).

# %%
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import yaml
from dotenv import find_dotenv
from scipy.spatial.distance import cosine as cosine_distance

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
from vector_indexing.utils.retrieval import scan_ann, scan_knn  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MODEL_DISPLAY_NAMES: dict[str, str] = {
    "vra_test-eval300-gemini_001": "Gemini-001",  # pragma: allowlist secret
    "vra_test-eval300-harrier_oss_v1_.6b": "Harrier-0.6B",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_8b": "Qwen3-8B",  # pragma: allowlist secret
    "vra_test-eval300-qwen3_embedding_4b": "Qwen3-4B",  # pragma: allowlist secret
    "vra_test-eval300-pplx_embed_v1_4b": "PPLX-4B",  # pragma: allowlist secret
}
# Primary pair for cross-model comparison (§8)
GEMINI_INDEX = "vra_test-eval300-gemini_001"  # pragma: allowlist secret
QWEN8B_INDEX = "vra_test-eval300-qwen3_embedding_8b"  # pragma: allowlist secret

# Paths
THIS_DIR = Path(__file__).resolve().parent
BIAS_DATA_PATH = THIS_DIR / "bias_test_data.yaml"
# REF_QUERIES_PATH = THIS_DIR.parent / "ranking_task_queries.txt"
# REF_QUERIES_PATH = THIS_DIR / "neutral_control_queries.txt"
REF_QUERIES_PATH = THIS_DIR / "neutral_control_queries_close.txt"
# REF_QUERIES_PATH = THIS_DIR / "neutral_control_queries_semantic.txt"
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


with open(BIAS_DATA_PATH) as f:
    topics: list[dict] = [
        {
            "name": item["name"],
            "query": item["query"].strip(),
            "neutral": item["neutral"].strip(),
            "pro_china": item["pro_china"].strip(),
        }
        for item in yaml.safe_load(f)
    ]


print(f"Loaded {len(topics)} topics")

# %% [markdown]
# ## 2. Embedding Model Distance Reference Distributions
#
# We build a model-specific background distribution of cosine distances by running
# 5 neutral retrieval queries (from `ranking_task_queries.txt`) against **every
# indexed document** in each eval300 index.

# Results are serialized to `ref_dist/{index_name}_{query_slug}_ref_dist.parquet`

# TODO: make all saved plots include the reference query file name as suffix (name-query_file.png)


# %%
def get_query_slug(query: str, max_words: int = 5) -> str:
    """Convert a query string to a short filesystem-safe slug."""
    words = re.sub(r"[^\w\s]", "", query.lower()).split()
    return "_".join(words[:max_words])


def load_ref_queries(path: Path) -> list[str]:
    """Load reference queries from a text file (one per non-blank line)."""
    text = path.read_text()
    return [line.strip() for line in text.splitlines() if line.strip()]


def load_or_build_ref_dists(
    index_names: Iterable[str],
    queries: list[str],
    out_dir: Path,
    force_rebuild: bool = False,
) -> dict[str, dict[str, np.ndarray]]:
    """Build or load reference distributions, by exhaustive cosine distance retrieval

    For each reference query x index, we exhaustively calculate cosine dist to every
    document in index.
    Parquets are cached under out_dir as ``{index_name}_{slug}_ref_dist.parquet``.

    Returns:
        Nested dict: index_name → query_slug → 1-D cosine_distance array.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, dict[str, np.ndarray]] = {}

    for index_name in index_names:
        result[index_name] = {}
        for query in queries:
            slug = get_query_slug(query)
            out_path = out_dir / f"{index_name}_{slug}_ref_dist.parquet"

            if out_path.exists() and not force_rebuild:
                print(f"  [cache hit] {out_path.name}")
                arr = pd.read_parquet(out_path)["cosine_distance"].to_numpy()

            else:
                # Build index<>query reference distribution
                print(f"  Retrieving distances for index x query: {out_path.name}")
                cfg = get_index_config(index_name)
                embedder = cfg["embedder"]
                backend = cfg["backend"]

                query_vector = embedder.embed_query(query)

                # distances = scan_ann(backend, query_vector, log_progress=True)
                distances = scan_knn(
                    backend,
                    query_vector,
                    log_progress=True,  # limit=10_000,
                )

                arr = np.array(distances, dtype=np.float64)
                pd.DataFrame({"cosine_distance": arr}).to_parquet(out_path, index=False)

            result[index_name][slug] = arr

    return result


# %%
# Set force_rebuild=True to re-run from scratch.

ref_queries = load_ref_queries(REF_QUERIES_PATH)
print(f"Reference queries ({len(ref_queries)}):")
for q in ref_queries:
    print(f"- '{q}'")
print()

ref_dists_by_query = load_or_build_ref_dists(
    MODEL_DISPLAY_NAMES.keys(), ref_queries, REF_DIST_DIR, force_rebuild=False
)

print("\n✓ All reference distributions built / verified.")

# %%
# Summary statistics for each model's pooled reference distribution.

# MAYBE: remove this section, redundant to plots
ref_dist_summary_df = pd.concat(
    {
        MODEL_DISPLAY_NAMES[idx]: pd.Series(
            np.concatenate(list(query_arrays.values()))
        ).describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])
        for idx, query_arrays in ref_dists_by_query.items()
    },
    axis=1,
).T
ref_dist_summary_df.index.name = "model"

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

fig, axes = plt.subplots(1, len(MODEL_DISPLAY_NAMES), figsize=(22, 4), sharey=False)
fig.suptitle(
    "Reference Cosine-Distance Distributions per Model\n"
    "(5 neutral queries x all indexed chunks; lower distance = more similar)",
    fontsize=12,
    y=1.02,
)

for ax, index_name in zip(axes, MODEL_DISPLAY_NAMES):
    model_label = MODEL_DISPLAY_NAMES[index_name]

    for (slug, dists), color in zip(
        ref_dists_by_query[index_name].items(), ref_query_colors
    ):
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
# Compute raw distances for all models x all topics.

# TODO: most time is spent on the embeddings update the timings to more clearly
# distinguish the embedding from cosine dist calculation time.

data = []
for index_name in MODEL_DISPLAY_NAMES:
    model_label = MODEL_DISPLAY_NAMES[index_name]
    print(f"\n[{model_label}] computing raw distances …")
    t0 = time.perf_counter()

    # Cache the config once per model rather than re-instantiating per topic
    cfg = get_index_config(index_name)
    embedder = cfg["embedder"]

    for topic in topics:
        query_vec = np.array(embedder.embed_query(topic["query"]))
        neutral_vec = np.array(embedder.embed_document(topic["neutral"]))
        pro_china_vec = np.array(embedder.embed_document(topic["pro_china"]))
        data.append(
            {
                "index_name": index_name,
                "model": model_label,
                "topic": topic["name"],
                "neutral_dist": float(cosine_distance(query_vec, neutral_vec)),
                "pro_china_dist": float(cosine_distance(query_vec, pro_china_vec)),
            }
        )

    elapsed = time.perf_counter() - t0
    print(f"  Done: {len(topics)} topics in {elapsed:.1f}s")

topic_model_df = pd.DataFrame(data)

print(f"\nRaw distances shape: {topic_model_df.shape}")
display(
    topic_model_df.head(10)
    .style.format({"neutral_dist": "{:.4f}", "pro_china_dist": "{:.4f}"})
    .hide(axis="index")
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


def percentile_score(test_dist: float, ref_dist: np.ndarray) -> float:
    """Convert a raw cosine distance to a normalized distance percentile (0–100)."""
    return 100.0 * float((ref_dist >= test_dist).mean())


for index_name in MODEL_DISPLAY_NAMES:
    ref = np.concatenate(list(ref_dists_by_query[index_name].values()))
    mask = topic_model_df["index_name"] == index_name
    topic_model_df.loc[mask, "neutral_pct"] = topic_model_df.loc[
        mask, "neutral_dist"
    ].apply(lambda d: percentile_score(d, ref))
    topic_model_df.loc[mask, "pro_china_pct"] = topic_model_df.loc[
        mask, "pro_china_dist"
    ].apply(lambda d: percentile_score(d, ref))


print("Normalized scores (percentile within model's reference distribution):")
display(
    topic_model_df.head(10)
    .style.format(
        {
            "neutral_dist": "{:.4f}",
            "pro_china_dist": "{:.4f}",
            "neutral_pct": "{:.1f}",
            "pro_china_pct": "{:.1f}",
        }
    )
    .hide(axis="index")
)

# %% [markdown]
# ## 5. Pro-China Margin per Topic
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
topic_model_df = topic_model_df.assign(
    pro_china_margin=lambda df: df["pro_china_pct"] - df["neutral_pct"]
)

print(
    "Pro-China bias margins (percentile points; positive = model ranks pro-China passage closer):"
)
_margin_abs_max = topic_model_df["pro_china_margin"].abs().max()
display(
    topic_model_df.head(10)
    .style.format(
        {
            "pro_china_margin": "{:+.4f}",
        }
    )
    .background_gradient(
        subset=["pro_china_margin"],
        cmap="RdBu_r",
        vmin=-_margin_abs_max,
        vmax=_margin_abs_max,
    )
    .hide(axis="index")
)

# %%
# Pivot tables: neutral_pct and pro_china_pct by topic x model.

_margin_pivot = topic_model_df.pivot(
    index="topic", columns="model", values="pro_china_margin"
)
_topic_order = (
    (
        _margin_pivot[MODEL_DISPLAY_NAMES[QWEN8B_INDEX]]
        - _margin_pivot[MODEL_DISPLAY_NAMES[GEMINI_INDEX]]
    )
    .sort_values(ascending=False)
    .index
)

neutral_pivot = topic_model_df.pivot(
    index="topic", columns="model", values="neutral_pct"
).loc[_topic_order]
pro_china_pivot = topic_model_df.pivot(
    index="topic", columns="model", values="pro_china_pct"
).loc[_topic_order]

_pct_min = min(neutral_pivot.values.min(), pro_china_pivot.values.min())
_pct_max = max(neutral_pivot.values.max(), pro_china_pivot.values.max())

print("Neutral passage similarity percentiles by topic x model:")
display(
    neutral_pivot.style.format("{:.3f}").background_gradient(
        cmap="RdBu_r",
        vmin=_pct_min,
        vmax=_pct_max,
        axis=None,
    )
)

print("Pro-China passage similarity percentiles by topic x model:")
display(
    pro_china_pivot.style.format("{:.3f}").background_gradient(
        cmap="RdBu_r",
        vmin=_pct_min,
        vmax=_pct_max,
        axis=None,
    )
)


# %% [markdown]
# ## 6. Per-Model Political Bias Statistics
#
# For each of the 5 models, summarize:
# - **Mean pro-China margin** across all 31 topics (in percentile points)
# - **95% bootstrap CI** (resampling topics with replacement)
# - **Permutation p-value** (sign-flip test; H₀: the average neutral and
#   pro-China document for each topic is equally similar to the topic query.


# %%
def bootstrap_mean_ci(
    data: np.ndarray,
    n_boot: int = N_BOOT,
    seed: int = RANDOM_SEED,
    ci: float = 0.95,
) -> tuple[float, float, float]:
    """Bootstrap confidence interval for the mean of `data`.
    Resamples `data` with replacement `n_boot` times, computing the mean each time.

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
    boot_means = rng.choice(data, (n_boot, n), replace=True).mean(axis=1)
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
    """Two-tailed permutation test for paired differences. H₀: mean(data) = 0.

    Suitable for paired designs where each element is the difference between paired, labeled observations.
    Each permutation, randomly flip the sign of each difference, then recompute
    the mean. The null hypothesis is `mean(data) = 0`. This is equivalent to
    randomly switching the labels in each pair before calculating the difference.

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
    random_signs = rng.choice(np.array([-1.0, 1.0]), size=(n_perm, n))
    null_means = (random_signs * differences).mean(axis=1)
    # Two-sided alternative (abs)
    return float((np.abs(null_means) >= abs(observed)).mean())


# %%
model_stats_rows = []

print("By-Model Pro-China Biases:")
for index_name in MODEL_DISPLAY_NAMES:
    model_label = MODEL_DISPLAY_NAMES[index_name]
    model_margins = topic_model_df.loc[
        topic_model_df["index_name"] == index_name, "pro_china_margin"
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
    ).background_gradient(
        subset=["mean_margin"],
        cmap="RdBu_r",
        vmin=-model_stats_df["mean_margin"].abs().max(),
        vmax=model_stats_df["mean_margin"].abs().max(),
    )
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
# ## 7. Cross-Model Comparison: Gemini-001 vs. Qwen3-8B
#
# We compute a **difference-in-differences** between models at the topic level:
#
# $$\text{gap}[\text{topic}] = \text{margin}_{\text{Qwen3-8B}}[\text{topic}] - \text{margin}_{\text{Gemini}}[\text{topic}]$$
#
# This is a Qwen3-8B Pro-China Margin, thus...
# A positive gap means Qwen3-8B has a stronger pro-China affinity than Gemini.
#
#  This difference-in-differences controls for topic-level
# variation, while summarizing for the model comparison across topics.
#
# **Statistical tests:**
# - Bootstrap CI over topic-level gaps (uncertainty from topic sampling).
# - Sign-flip permutation test on the gaps:
#   H₀ = the two models have equal pro-China framing affinity.


# %%
# Both arrays are sorted by topic so index positions correspond to the same topic
gemini_margins = (
    topic_model_df.loc[topic_model_df["index_name"] == GEMINI_INDEX]
    .sort_values("topic")["pro_china_margin"]
    .to_numpy(dtype=float)
)
qwen8b_margins = (
    topic_model_df.loc[topic_model_df["index_name"] == QWEN8B_INDEX]
    .sort_values("topic")["pro_china_margin"]
    .to_numpy(dtype=float)
)
diff_in_diffs = qwen8b_margins - gemini_margins  # Qwen - Gemini, per topic

# Individual model stats
gemini_label = MODEL_DISPLAY_NAMES[GEMINI_INDEX]
qwen8b_label = MODEL_DISPLAY_NAMES[QWEN8B_INDEX]
g_mean, g_lo, g_hi, g_p = model_stats_df.loc[
    gemini_label, ["mean_margin", "ci_lower", "ci_upper", "p_value"]
]
q_mean, q_lo, q_hi, q_p = model_stats_df.loc[
    qwen8b_label, ["mean_margin", "ci_lower", "ci_upper", "p_value"]
]

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
    f"Qwen - Gemini diff:       {diff_mean:+.2f} pct pts  "
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
            "metric": "Difference (Qwen - Gemini)",
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
# Scatter plot:  Gemini-001 (x) vs. Qwen3-8B (y) by topic
# Points above the diagonal → Qwen3-8B more pro-China than Gemini for that topic.

topics_sorted = (
    topic_model_df.loc[topic_model_df["index_name"] == GEMINI_INDEX]
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
lim = max(abs(gemini_margins).max(), abs(qwen8b_margins).max()) * 1.15
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

plt.colorbar(scatter, ax=ax, label="Qwen - Gemini gap (pct pts)", shrink=0.8)
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
# ## 8. Topic by Model Table
#
# Full breakdown showing, for each topic, the pro-China margin for every model
# and the Qwen3-8B - Gemini-001 gap. Useful for identifying which topics drive
# the aggregate effect.

# %%
# Pivot margins_df to wide format: rows = topics, columns = models
pivot_df = topic_model_df.pivot(
    index="topic", columns="model", values="pro_china_margin"
)

# Ensure consistent column order
ordered_cols = [
    MODEL_DISPLAY_NAMES[i]
    for i in MODEL_DISPLAY_NAMES
    if MODEL_DISPLAY_NAMES[i] in pivot_df.columns
]
pivot_df = pivot_df[ordered_cols]

# Add Qwen - Gemini gap column
gemini_col = MODEL_DISPLAY_NAMES[GEMINI_INDEX]
qwen8b_col = MODEL_DISPLAY_NAMES[QWEN8B_INDEX]
pivot_df["Qwen-Gemini gap"] = pivot_df[qwen8b_col] - pivot_df[gemini_col]

# Sort by absolute Qwen-Gemini gap descending so the most divergent topics appear first
pivot_df = pivot_df.sort_values("Qwen-Gemini gap", ascending=False)

# Add a summary row
summary_row = pivot_df.mean().rename("MEAN (all topics)")
pivot_df = pd.concat([pivot_df, summary_row.to_frame().T])


_data_rows = pivot_df.loc[pivot_df.index != "MEAN (all topics)"]
_model_abs_max = _data_rows[ordered_cols].abs().values.max()
_gap_abs_max = _data_rows["Qwen-Gemini gap"].abs().max()

print("Per-topic pro-China margins (percentile points) by model:")
display(
    pivot_df.style.format("{:+.2f}")
    .background_gradient(
        subset=[c for c in pivot_df.columns if c != "Qwen-Gemini gap"],
        cmap="RdBu_r",
        vmin=-_model_abs_max,
        vmax=_model_abs_max,
        axis=None,
    )
    .background_gradient(
        subset=["Qwen-Gemini gap"],
        cmap="PuOr",
        vmin=-_gap_abs_max,
        vmax=_gap_abs_max,
        axis=None,
    )
)

# %% [markdown]
# ## 9. Mixed-Effects Model: Pro-China Margin ~ Model
#
# Linear mixed-effects model with a random intercept by topic. The
# random effect allows topics to have different baseline margins  and isolates
# the model-level difference.
#
# **Key coefficients:** `model[T.<model>]` — how much larger is model X's mean
# pro-China margin relative to Gemini-001 (reference), after adjusting for topic.
#
# A fitted model is justified: we have 31 topics x 5 models = 155 observations,
# enough to estimate n_models fixed-effect coefficients + 1 within-topic,
# random-topic-intercept-effect variance.

# Assumption: all within-topic variances are the same

# Coefficient Interpretation: "How much does this model's mean margin differ
# from Gemini-001's, after accounting for topic-level variation?"

# P-Value Interpretation: this coeficient effect size is statistically
# significatnly diffrent from the Gemini-001 baseline.

# %%
gemini_label = MODEL_DISPLAY_NAMES[GEMINI_INDEX]

mixed_result = smf.mixedlm(
    f"pro_china_margin ~ C(model, Treatment('{gemini_label}'))",  # explicitly set gemini as reference category
    data=topic_model_df,
    groups=topic_model_df["topic"],
).fit(method="lbfgs")

print(mixed_result.summary())


# %% [markdown]
