#!/usr/bin/env python

# To convert: Cmd+Shift+P → "Jupyter: Export Current Python File as Jupyter Notebook"

# %% [markdown]
# # Ranking Task Analysis — Embedding Model Ranking Aggregation
#
# Computes **pairwise win-rates** and **Kemeny-Young consensus rankings** across
# embedding models from a human-annotated ranking workbook.
#
# **Pipeline overview**
# 1. Load & parse all task sheets from the Excel workbook
# 2. Validate ratings (incomplete rows, same-chunk conflicts, different-chunk ties)
# 3. Remove invalid rows and compute pairwise scores
# 4. Aggregate pairwise win-rates per query and per model
# 5. Brute-force Kemeny-Young consensus ranking per query and overall
# 6. Save results to CSV

# %%
from __future__ import annotations

from itertools import permutations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# %% [markdown]
# ## Configuration
#
# - **`DATA_LOADING_CONFIG`** — workbook path, column-to-model mapping, and sheet parsing offsets
# - **`PAIRWISE_WINRATE_CONFIG`** — scoring convention and tie handling
# - **`OUTPUT_CONFIG`** — directory where CSVs are written

# %%
# Data-loading configuration (workbook shape + model mapping).
DATA_LOADING_CONFIG = {
    "workbook_path": Path(__file__).parent / "Ranking Task - Embedding Models.xlsx",
    "column_to_model": {
        # Excel columns B..F become pandas positional columns 1..5 (0-based).
        1: "pplx_embed_v1_4b",
        2: "qwen3_embedding_4b",
        3: "qwen3_embedding_8b",
        4: "harrier_oss_v1_.6b",
        5: "gemini-embedding-001",
    },
    "query_cell": (1, 1),  # row, col (0-based in pandas frame loaded with header=None)
    "chunk_id_row": 4,  # row index holding chunk ids
    "rater_start_row": 7,  # first row containing rater names/scores
    "rater_col": 0,  # column index containing rater names
    "task_sheet_prefix": "Task ",
    "exclude_sheet_names": ["Task template"],
}

# Pairwise win-rate configuration (algorithm behavior).
PAIRWISE_WINRATE_CONFIG = {
    "lower_rank_is_better": True,  # TODO: remove this config, its logic should be hard coded, rankings semantics are intrinsic
    "tie_score": 0.5,
}

# Kemeny-Young consensus ranking configuration.
KEMENY_CONFIG = {
    "tie_score": 0.0,  # ties are indifferent — no disagreement cost (standard Kemeny convention)
}

# Output configuration (artifacts only).
OUTPUT_CONFIG = {
    "output_dir": Path(__file__).parent / "ranking_task_results",
}


# %% [markdown]
# ## Data Loading
#
# Reads every sheet whose name starts with the configured prefix, parses rater rankings
# and chunk IDs, and returns:
# - **`rank_df`** — one row per (sheet, rater), with a rank column per model
# - **`chunk_df`** — one row per (sheet, model), recording which chunk was shown
# - **`model_columns`** — ordered list of model names


# %%
# TODO: inline this with comment, its used once
def task_sheet_names(
    workbook_path: Path,
    *,
    sheet_prefix: str,
    exclude_sheet_names: list[str] | None = None,
) -> list[str]:
    """Return task sheet names matching prefix and exclusions."""
    workbook = pd.ExcelFile(workbook_path, engine="openpyxl")
    excluded = set(exclude_sheet_names or [])
    return [
        sheet_name
        for sheet_name in workbook.sheet_names
        if sheet_name.startswith(sheet_prefix) and sheet_name not in excluded
    ]


def read_task_sheet(
    workbook_path: Path,
    sheet_name: str,
    *,
    column_to_model: dict[int, str],
    query_cell: tuple[int, int],
    chunk_id_row: int,
    rater_start_row: int,
    rater_col: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse one sheet into rank rows and chunk-id rows."""
    raw = pd.read_excel(
        workbook_path,
        sheet_name=sheet_name,
        header=None,
        engine="openpyxl",
    )
    query_row, query_col = query_cell
    query = raw.iat[query_row, query_col]

    model_columns = list(column_to_model.values())
    score_col_indices = list(column_to_model.keys())
    rank_df = raw.iloc[rater_start_row:, [rater_col, *score_col_indices]].copy()
    rank_df.columns = ["rater", *model_columns]
    rank_df["sheet_row"] = rank_df.index + 1  # Excel-like 1-based row number
    rank_df["sheet_name"] = sheet_name
    rank_df["query"] = query
    rank_df = rank_df[rank_df["rater"].notna()].reset_index(drop=True)
    rank_df[model_columns] = rank_df[model_columns].apply(
        pd.to_numeric, errors="coerce"
    )
    rank_df["has_any_rating"] = rank_df[model_columns].notna().any(axis=1)
    rank_df["is_complete"] = rank_df[model_columns].notna().all(axis=1)
    rank_df = rank_df[
        [
            "sheet_name",
            "query",
            "sheet_row",
            "rater",
            *model_columns,
            "has_any_rating",
            "is_complete",
        ]
    ]

    chunk_df = pd.DataFrame(
        {
            "sheet_name": sheet_name,
            "query": query,
            "model": model_columns,
            "chunk_id": [raw.iat[chunk_id_row, idx] for idx in score_col_indices],
        }
    )
    return rank_df, chunk_df


def load_rank_data(
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Load and union all task sheets into normalized dataframes."""
    workbook_path = config["workbook_path"]
    model_columns = list(config["column_to_model"].values())
    sheets = task_sheet_names(
        workbook_path,
        sheet_prefix=config["task_sheet_prefix"],
        exclude_sheet_names=config["exclude_sheet_names"],
    )
    per_sheet = [
        read_task_sheet(
            workbook_path,
            sheet_name,
            column_to_model=config["column_to_model"],
            query_cell=config["query_cell"],
            chunk_id_row=config["chunk_id_row"],
            rater_start_row=config["rater_start_row"],
            rater_col=config["rater_col"],
        )
        for sheet_name in sheets
    ]
    rank_df = pd.concat([item[0] for item in per_sheet], ignore_index=True)
    chunk_df = pd.concat([item[1] for item in per_sheet], ignore_index=True)
    return rank_df, chunk_df, model_columns


# %% [markdown]
# ## Data Validation
#
# Three checks flag data quality issues before scoring:
#
# | Check | Description |
# |---|---|
# | `incomplete_rows` | Rater provided at least one rank but not all |
# | `same_chunk_different_rating` | Two models showed the same chunk but received different ranks |
# | `different_chunk_same_rating` | Two models showed different chunks but received the same rank |
#
# Rows that fail any check are excluded from rank aggregation scoring.


# %%
def model_combinations(model_columns: list[str]) -> pd.DataFrame:
    """Return all unique model combinations with stable order."""
    order_df = pd.DataFrame(
        {"model": model_columns, "model_order": range(len(model_columns))}
    )
    combos = order_df.merge(order_df, how="cross", suffixes=("_a", "_b"))
    combos = combos[combos["model_order_a"] < combos["model_order_b"]].reset_index(
        drop=True
    )
    return combos.rename(columns={"model_a": "model_a", "model_b": "model_b"})[
        ["model_a", "model_b", "model_order_a", "model_order_b"]
    ]


def model_combination_rows(
    rank_df: pd.DataFrame, model_columns: list[str]
) -> pd.DataFrame:
    """Create per-row model-combination rank rows."""
    id_cols = ["sheet_name", "query", "sheet_row", "rater"]
    order_df = pd.DataFrame(
        {"model": model_columns, "model_order": range(len(model_columns))}
    )
    long_df = rank_df.melt(
        id_vars=id_cols,
        value_vars=model_columns,
        var_name="model",
        value_name="rank",
    ).merge(order_df, on="model", how="left")

    left = long_df.rename(
        columns={"model": "model_a", "rank": "rank_a", "model_order": "model_order_a"}
    )
    right = long_df.rename(
        columns={"model": "model_b", "rank": "rank_b", "model_order": "model_order_b"}
    )
    combos = left.merge(right, on=id_cols, how="inner")
    combos = combos[combos["model_order_a"] < combos["model_order_b"]].reset_index(
        drop=True
    )
    return combos[
        [
            "sheet_name",
            "query",
            "sheet_row",
            "rater",
            "model_a",
            "model_b",
            "rank_a",
            "rank_b",
        ]
    ]


def chunk_combination_rules(
    chunk_df: pd.DataFrame, model_columns: list[str]
) -> pd.DataFrame:
    """Create per-query model-combination chunk rules."""
    order_df = pd.DataFrame(
        {"model": model_columns, "model_order": range(len(model_columns))}
    )
    chunk_ordered = chunk_df.merge(order_df, on="model", how="left")
    left = chunk_ordered.rename(
        columns={
            "model": "model_a",
            "chunk_id": "chunk_id_a",
            "model_order": "model_order_a",
        }
    )
    right = chunk_ordered.rename(
        columns={
            "model": "model_b",
            "chunk_id": "chunk_id_b",
            "model_order": "model_order_b",
        }
    )
    rules = left.merge(right, on=["sheet_name", "query"], how="inner")
    rules = rules[rules["model_order_a"] < rules["model_order_b"]].copy()
    rules["same_chunk"] = rules["chunk_id_a"] == rules["chunk_id_b"]
    return rules[["sheet_name", "query", "model_a", "model_b", "same_chunk"]]


def build_invalid_rows(
    rank_df: pd.DataFrame, chunk_df: pd.DataFrame, model_columns: list[str]
) -> pd.DataFrame:
    """Return one row per (source row x check) for each invalid source row.

    Each check deduplicates to unique (sheet_name, sheet_row, rater) keys before
    being tagged, so pairwise-combination fan-out never inflates the count.
    """
    chunk_rules_df = chunk_combination_rules(chunk_df, model_columns)
    source_key_cols = ["sheet_name", "sheet_row", "rater"]

    # Incomplete rows: rater started but did not finish all model ratings.
    incomplete = (
        rank_df.loc[
            rank_df["has_any_rating"] & ~rank_df["is_complete"], source_key_cols
        ]
        .drop_duplicates()
        .assign(check_name="incomplete_rows")
    )

    # Pairwise checks apply only to fully-complete rows.
    combo_rows = model_combination_rows(
        rank_df.loc[rank_df["is_complete"]], model_columns
    )
    joined = combo_rows.merge(
        chunk_rules_df, on=["sheet_name", "query", "model_a", "model_b"], how="left"
    )

    # Same chunk shown to rater but given different ranks — rating is inconsistent.
    same_chunk_diff = (
        joined.loc[
            joined["same_chunk"] & (joined["rank_a"] != joined["rank_b"]),
            source_key_cols,
        ]
        .drop_duplicates()
        .assign(check_name="same_chunk_different_rating")
    )

    # Different chunks given the same rank — rater did not distinguish them.
    diff_chunk_same = (
        joined.loc[
            ~joined["same_chunk"] & (joined["rank_a"] == joined["rank_b"]),
            source_key_cols,
        ]
        .drop_duplicates()
        .assign(check_name="different_chunk_same_rating")
    )

    frames = [f for f in [incomplete, same_chunk_diff, diff_chunk_same] if not f.empty]
    if not frames:
        return pd.DataFrame(columns=[*source_key_cols, "check_name"])

    result = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates()
        .sort_values([*source_key_cols, "check_name"])
        .reset_index(drop=True)
    )
    result["sheet_row"] = result["sheet_row"].astype(int)
    return result


def filter_valid_rank_rows(
    rank_df: pd.DataFrame, removed_rows_df: pd.DataFrame
) -> pd.DataFrame:
    """Remove invalid rows and return valid rank data."""
    if removed_rows_df.empty:
        return rank_df.copy()

    keys = removed_rows_df[["sheet_name", "sheet_row", "rater"]].drop_duplicates()
    filtered = rank_df.merge(
        keys, on=["sheet_name", "sheet_row", "rater"], how="left", indicator=True
    )
    return filtered.loc[filtered["_merge"] == "left_only", rank_df.columns].reset_index(
        drop=True
    )


# %% [markdown]
# ## Pairwise Win Rate Rank Aggregation
#
# For each (2 choose M) model combinations (A, B):
# For each (rater, query) that ranked that model combination:
# We assign a win score to each model:
# - **A wins** → `score_a = 1.0`, `score_b = 0.0`
# - **B wins** → `score_a = 0.0`, `score_b = 1.0`
# - **Tie** → both receive `tie_score` (default `0.5`)
#
# Scores are averaged across all rater rows to produce **pairwise win-rates**,
# aggregated both per-query and overall per-model.


# %%
def pairwise_scores_df(
    rank_df: pd.DataFrame,
    model_columns: list[str],
    *,
    lower_rank_is_better: bool,
    tie_score: float,
) -> pd.DataFrame:
    """Build pairwise score rows from normalized rank data."""
    combos = (
        model_combination_rows(rank_df, model_columns)
        .dropna(subset=["rank_a", "rank_b"])
        .copy()
    )
    if combos.empty:
        return pd.DataFrame(
            columns=[
                "sheet_name",
                "query",
                "sheet_row",
                "rater",
                "model_a",
                "model_b",
                "rank_a",
                "rank_b",
                "score_a",
                "score_b",
            ]
        )

    if lower_rank_is_better:
        a_wins = combos["rank_a"] < combos["rank_b"]
        b_wins = combos["rank_a"] > combos["rank_b"]
    else:
        a_wins = combos["rank_a"] > combos["rank_b"]
        b_wins = combos["rank_a"] < combos["rank_b"]

    combos["score_a"] = np.select([a_wins, b_wins], [1.0, 0.0], default=tie_score)
    combos["score_b"] = np.select([a_wins, b_wins], [0.0, 1.0], default=tie_score)
    return combos[
        [
            "sheet_name",
            "query",
            "sheet_row",
            "rater",
            "model_a",
            "model_b",
            "rank_a",
            "rank_b",
            "score_a",
            "score_b",
        ]
    ].reset_index(drop=True)


def _pairwise_to_long(pairwise_df: pd.DataFrame) -> pd.DataFrame:
    """Unpivot pairwise scores into a long (query, model, score) frame."""
    a_side = pairwise_df.rename(columns={"model_a": "model", "score_a": "score"})[
        ["query", "model", "score"]
    ]
    b_side = pairwise_df.rename(columns={"model_b": "model", "score_b": "score"})[
        ["query", "model", "score"]
    ]
    return pd.concat([a_side, b_side], ignore_index=True)


def aggregate_by_query(pairwise_scores_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate model pairwise win scores by query."""
    if pairwise_scores_df.empty:
        return pd.DataFrame(
            columns=[
                "query",
                "model",
                "pairwise_win_rate",
                "ties",
                "comparisons",
                "rank_within_query",
            ]
        )

    long_df = _pairwise_to_long(pairwise_scores_df)

    query_scores = (
        long_df.groupby(["query", "model"], as_index=False)
        .agg(
            pairwise_win_rate=("score", "mean"),
            ties=("score", lambda s: (s == 0.5).sum()),
            comparisons=("score", "size"),
            total_score=("score", "sum"),
        )
        .sort_values(
            ["query", "pairwise_win_rate", "total_score", "model"],
            ascending=[True, False, False, True],
        )
        .reset_index(drop=True)
    )
    query_scores["rank_within_query"] = (
        query_scores.groupby("query")["pairwise_win_rate"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    return query_scores


# TODO: future: if we have some queries/tasks with many more ratings than others, \
# first agg by task, then agg by model, so that the highly rated tasks do not \
# out weigh the rarely rated tasks in the mean
def aggregate_by_model(pairwise_scores_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate model pairwise win scores at model level."""
    if pairwise_scores_df.empty:
        return pd.DataFrame(
            columns=["model", "pairwise_win_rate", "comparisons", "overall_rank"]
        )

    long_df = _pairwise_to_long(pairwise_scores_df)

    model_scores = (
        long_df.groupby("model", as_index=False)
        .agg(
            pairwise_win_rate=("score", "mean"),
            ties=("score", lambda s: (s == 0.5).sum()),
            comparisons=("score", "size"),
            total_score=("score", "sum"),
            queries_count=("query", "nunique"),
        )
        .sort_values(
            ["pairwise_win_rate", "total_score", "model"],
            ascending=[False, False, True],
        )
        .reset_index(drop=True)
    )
    model_scores["overall_rank"] = (
        model_scores["pairwise_win_rate"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    return model_scores


# %% [markdown]
# ## Brute-Force Kemeny-Young Consensus Ranking
#
# Finds the consensus ranking that is least different to given rankings.
# The consensus ranking minimizes the **Kemeny score** — the sum of pairwise
# candidate comparison disagreement between 2 rankings - with all raters.
#
# For M models there are M! possible rankings. We use brute force, to calculate
# the Kemeny score for each possible ranking and return the ranking with the lowest score.
#
# **Tie handling:** same-chunk ties carry a `tie_score=0.0` (indifference, no disagreement
# cost). This differs from the pairwise win-rate's `tie_score=0.5`.


# %%
def build_preference_matrix(
    pairwise_df: pd.DataFrame, model_columns: list[str]
) -> np.ndarray:
    """Build W[i][j] = total pairwise score voting for model i over model j.

    Inherits tie convention from the score_a/score_b values in pairwise_df.
    """
    n = len(model_columns)
    model_to_idx = {m: i for i, m in enumerate(model_columns)}
    W = np.zeros((n, n))
    a_idx = pairwise_df["model_a"].map(model_to_idx).to_numpy(dtype=int)
    b_idx = pairwise_df["model_b"].map(model_to_idx).to_numpy(dtype=int)
    np.add.at(W, (a_idx, b_idx), pairwise_df["score_a"].to_numpy())
    np.add.at(W, (b_idx, a_idx), pairwise_df["score_b"].to_numpy())
    return W


def _kemeny_score(permutation: tuple[int, ...], W: np.ndarray) -> float:
    """Kemeny score for one candidate permutation against preference matrix W.

    Score = sum of W[j, i] for every pair (i, j) where i appears before j in
    the permutation — i.e. the total votes that *disagree* with the candidate ordering.
    """
    score = 0.0
    n = len(permutation)
    for pos_i in range(n):
        for pos_j in range(pos_i + 1, n):
            score += W[permutation[pos_j], permutation[pos_i]]
    return score


def brute_force_kemeny(
    W: np.ndarray, model_columns: list[str]
) -> tuple[tuple[int, ...], float]:
    """Enumerate all M! permutations and return the one with minimum Kemeny score."""
    min_score = np.inf
    best_perm: tuple[int, ...] = tuple(range(len(model_columns)))
    for perm in permutations(range(len(model_columns))):
        score = _kemeny_score(perm, W)
        if score < min_score:
            min_score = score
            best_perm = perm
    return best_perm, min_score


def kemeny_consensus_by_query(
    pairwise_df: pd.DataFrame, model_columns: list[str]
) -> pd.DataFrame:
    """Run brute-force Kemeny-Young per query; return consensus ranks."""
    rows = []
    for query, group in pairwise_df.groupby("query"):
        W = build_preference_matrix(group, model_columns)
        best_perm, score = brute_force_kemeny(W, model_columns)
        for rank, model_idx in enumerate(best_perm, start=1):
            rows.append(
                {
                    "query": query,
                    "model": model_columns[model_idx],
                    "consensus_rank": rank,
                    "kemeny_score": score,
                }
            )
    return (
        pd.DataFrame(rows)
        .sort_values(["query", "consensus_rank"])
        .reset_index(drop=True)
    )


def kemeny_consensus_overall(
    pairwise_df: pd.DataFrame, model_columns: list[str]
) -> pd.DataFrame:
    """Run brute-force Kemeny-Young across all queries; return overall consensus ranks."""
    W = build_preference_matrix(pairwise_df, model_columns)
    best_perm, score = brute_force_kemeny(W, model_columns)
    return pd.DataFrame(
        [
            {"model": model_columns[i], "consensus_rank": rank, "kemeny_score": score}
            for rank, i in enumerate(best_perm, start=1)
        ]
    )


# %% [markdown]
# ## Execution

# %% [markdown]
# ### Load rank data

# %%
rank_df, chunk_df, model_columns = load_rank_data(DATA_LOADING_CONFIG)
rank_df


# %% [markdown]
# ### Identify invalid rows

# %%
invalid_rows = build_invalid_rows(rank_df, chunk_df, model_columns)
invalid_rows


# %% [markdown]
# ### Filter invalid rows

# %%
if invalid_rows.empty:
    print("No invalid rows detected. Scoring all rated rows.")
else:
    print("Removing invalid rows before scoring:")
    print(invalid_rows.to_string(index=False))

valid_rank_df = filter_valid_rank_rows(rank_df, invalid_rows)

# %% [markdown]
# ## Pairwise Win Rate Rank Aggregation

# %% [markdown]
# ### Pairwise scores (raw)

# %%
pairwise_df = pairwise_scores_df(
    valid_rank_df,
    model_columns,
    lower_rank_is_better=PAIRWISE_WINRATE_CONFIG["lower_rank_is_better"],
    tie_score=PAIRWISE_WINRATE_CONFIG["tie_score"],
)
pairwise_df


# %% [markdown]
# ### Win-rate by query

# %%
query_level_ranking = aggregate_by_query(pairwise_df)
query_level_ranking

# TODO: plot model score faceted by query, bar plot

# %% [markdown]
# ### Win-rate by model (overall)

# %%
model_level_ranking = aggregate_by_model(pairwise_df)
model_level_ranking

# %%

# TODO: heat plot of pairwise win rate by model (upper triangle)

# %% [markdown]
# ## Brute-Force Kemeny-Young Consensus Ranking

# %% [markdown]
# ### Build Kemeny pairwise scores (tie_score = 0.0)

# %%
kemeny_pairwise_df = pairwise_scores_df(
    valid_rank_df,
    model_columns,
    lower_rank_is_better=PAIRWISE_WINRATE_CONFIG["lower_rank_is_better"],
    tie_score=KEMENY_CONFIG["tie_score"],
)
kemeny_pairwise_df

# %% [markdown]
# ### Consensus ranking by query

# %%
kemeny_by_query = kemeny_consensus_by_query(kemeny_pairwise_df, model_columns)
kemeny_by_query

# Note: the kemeny scores by query validate, human intuition about which queries \
# produce more equality results across models and which have obvious winners. \
# Higher kemeny score seems to suggest more equal chunks.

# TODO: normalize kemeny score to the number of participating rankers, so that \
# scores are comparable btw query and model aggregation

# %% [markdown]
# ### Consensus ranking overall

# %%
kemeny_overall = kemeny_consensus_overall(kemeny_pairwise_df, model_columns)
kemeny_overall
# %%
# TODO: BT/Elo ranking

# %%
# TODO: interrater reliability/dispersion metric (measure of the similarity of ratings/raters)
# kendalls W
# figure out how to visualize the inter-rater dispersion. aggregates by rater by
# model, to see whether people tend to rate the same model the same way

# I want a sense of the variability of ranking by query
# rank correlation btw query-level rankings

# %%
# TODO: some kind of confidence interval, and statistical significance test, \
# and power test for for the rank aggregation. This will help get a sense of how \
# many rankings per pairing or per query really necessary to get an informative score.

# %% [markdown]
# ## LLM-as-Judge rankings

# %%
# Display llm-as-judge query and model level results

LLM_JUDGE_RESULTS_PATH = (
    Path(__file__).parent
    / "llm_as_judge_results"
    / "llm_eval_retrieval_20260515T001055Z.json"
)

import json

with open(LLM_JUDGE_RESULTS_PATH) as f:
    _llm_judge_data = json.load(f)
llm_judge_raw_df = pd.DataFrame(_llm_judge_data["test_case_data"])


# Query-level: one row per (index, query), score from judge
llm_judge_query_df = (
    llm_judge_raw_df[["index_name", "query", "score"]]
    .rename(
        columns={
            "index_name": "model",
        }
    )
    .sort_values(["query", "score"], ascending=[True, False])
    .reset_index(drop=True)
)
# add rank
llm_judge_query_df["rank_within_query"] = (
    llm_judge_query_df.groupby("query")["score"]
    .rank(method="min", ascending=False)
    .astype(int)
)
llm_judge_query_df

# NOTE: llm-as-judge scoring has good correlation to pair-wise win rate

# TODO: plot model score faceted by query, bar plot

# TODO: calculate test case score from mean of chunk score with std dev (to get better sense of dispersion)

# %%
# Model-level: one row per model/index, mean score across all queries
llm_judge_model_df = (
    llm_judge_raw_df.groupby("index_name", as_index=False)
    .agg(
        mean_score=("score", "mean"),
        stdv_score=("score", "std"),
        queries_count=("query", "nunique"),
    )
    .rename(columns={"index_name": "model"})
    .sort_values("mean_score", ascending=False)
    .reset_index(drop=True)
)
llm_judge_model_df["rank"] = (
    llm_judge_model_df["mean_score"].rank(method="min", ascending=False).astype(int)
)
llm_judge_model_df

# %%
# TODO: spearnman rho or kendall tau rank correlation btw llm-as-judge and human raters aggregate. Overall, and query by query
# also plots faceted by query and llm-as-judge and different human rater aggregates


# %% [markdown]
# ### Save results

# %%
output_dir = OUTPUT_CONFIG["output_dir"]
output_dir.mkdir(parents=True, exist_ok=True)

pairwise_df.to_csv(output_dir / "pairwise_scores.csv", index=False)
query_level_ranking.to_csv(output_dir / "pairwise_win_rate_by_query.csv", index=False)
model_level_ranking.to_csv(output_dir / "pairwise_win_rate_by_model.csv", index=False)
invalid_rows.to_csv(output_dir / "pairwise_removed_invalid_rows.csv", index=False)
kemeny_by_query.to_csv(output_dir / "kemeny_consensus_by_query.csv", index=False)
kemeny_overall.to_csv(output_dir / "kemeny_consensus_overall.csv", index=False)
llm_judge_query_df.to_csv(output_dir / "llm_judge_by_query.csv", index=False)
llm_judge_model_df.to_csv(output_dir / "llm_judge_by_model.csv", index=False)

print(f"Wrote: {output_dir / 'pairwise_scores.csv'}")
print(f"Wrote: {output_dir / 'pairwise_win_rate_by_query.csv'}")
print(f"Wrote: {output_dir / 'pairwise_win_rate_by_model.csv'}")
print(f"Wrote: {output_dir / 'pairwise_removed_invalid_rows.csv'}")
print(f"Wrote: {output_dir / 'kemeny_consensus_by_query.csv'}")
print(f"Wrote: {output_dir / 'kemeny_consensus_overall.csv'}")
print(f"Wrote: {output_dir / 'llm_judge_by_query.csv'}")
print(f"Wrote: {output_dir / 'llm_judge_by_model.csv'}")


# %% [markdown]
# ## References
# - LLM rankings paper - https://arxiv.org/html/2412.09569v1#A2
# - Chatbot Arena LLM rankings - https://colab.research.google.com/drive/1KdwokPjirkTmpO_P1WByFNFiqxWQquwH#scrollTo=B_PYA7oVyaHO
# - Kemeny consensus optimization - https://vene.ro/blog/kemeny-young-optimal-rank-aggregation-in-python.html
