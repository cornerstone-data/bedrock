"""Step 5 of epic #337 (Option 2 refined): cell-level A comparison at BEA
summary aggregation, ranked by weighted RMSE per (approach, year).

The original Step 5 spec — comparing the model's saved ``q`` vector to BEA
gross output — turned out not to test A: ``q`` is produced by
``scale_cornerstone_q`` + ``inflate_cornerstone_q_or_y``, helpers that run
in parallel to A and never depend on it.

The first reframing (Option 2) tried to compare ``col_sum(A)`` to BEA
observed II/GO share per industry. That ran into a data wall (annual II
isn't in the codebase at detail level) AND was column-sum-only — a cell
with the wrong row distribution but right column total would pass.

This module ships **Option 2 refined**: it uses BEA's *summary-level* A
matrix, available annually via ``derive_summary_Adom_usa(year)`` +
``derive_summary_Aimp_usa(year)``, as ground truth. For each
(approach, year) it aggregates the Cornerstone detail A to summary level
via observed dollar-flow weighting (``Z = A · diag(q)`` then groupby
summary parent, then divide by aggregated q) and compares to the BEA
observed summary A cell by cell. Tests A's row distribution AND column
structure at summary granularity — column-sum caveat resolved.

See [issue #344 reframing comment](https://github.com/cornerstone-data/bedrock/issues/344#issuecomment-4357342817).

Reads the ``A_{approach}_{year}.parquet`` and ``q_{approach}_{year}.parquet``
caches from Step 1 and the BEA summary A from
``bedrock.transform.eeio.derived_2017``. Produces:

- ``summary_a_errors.csv`` — one row per (approach, year, dom_or_imp) with
  ``rmse_vs_bea_summary_a`` (Z-magnitude weighted), ``mean_abs_diff``,
  ``top_5_worst_cells`` (semicolon-joined ``ROW->COL:diff`` pairs).

- ``summary_a_rmse_ranking.png`` — grouped bar chart, x = year, five bars
  per group (one per approach), y = weighted RMSE for the combined
  (dom + imp) A matrix.

- Sheet tab ``summary_a_errors`` appended to the run-report Sheet.

**Caveat:** weighting and aggregation use each approach's own ``q``
vector (from the parquet cache). Approaches with poorer q-scaling get a
slightly biased weight. A future refinement would use BEA observed
detail GO as the common weight — pending a Cornerstone-detail-to-BEA-
detail GO split for sectors where Cornerstone disaggregates.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.summary_a_errors
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series._run_report import publish_tabs
from bedrock.analysis.a_matrix_time_series.constants import (
    APPROACH_COLORS,
    APPROACH_ORDER,
    APPROACH_YEAR_COVERAGE,
    PLOTS_DIR,
    RESULTS_DIR,
)
from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Adom_usa,
    derive_summary_Aimp_usa,
)
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)

logger = logging.getLogger(__name__)

SUMMARY_A_ERROR_YEARS: tuple[int, ...] = (
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
)

TOP_K_WORST_CELLS_REPORTED = 5


def _cornerstone_to_summary() -> dict[str, str]:
    """Invert ``load_bea_v2017_summary_to_cornerstone`` into a
    cornerstone_code → bea_summary_code lookup."""
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    return {
        code: str(summary)
        for summary, codes in summary_to_cornerstone.items()
        for code in codes
    }


def _load_a_total_dom_plus_imp(approach: str, year: int) -> pd.DataFrame:
    """A_total = A_dom + A_imp, str-typed indices."""
    combined = pd.read_parquet(RESULTS_DIR / f"A_{approach}_{year}.parquet")
    adom = pd.DataFrame(combined.loc["dom"])
    aimp = pd.DataFrame(combined.loc["imp"])
    a = adom.add(aimp, fill_value=0.0)
    a.index = a.index.astype(str)
    a.columns = a.columns.astype(str)
    return a


def _load_q_detail(approach: str, year: int) -> pd.Series:
    df = pd.read_parquet(RESULTS_DIR / f"q_{approach}_{year}.parquet")
    series = pd.Series(df["q"].astype(float))
    series.index = series.index.astype(str)
    return series


def aggregate_detail_a_to_summary(
    a_detail: pd.DataFrame, q_detail: pd.Series, cs_to_summary: dict[str, str]
) -> pd.DataFrame:
    """Aggregate detail-level A to summary level via dollar-flow weighting.

    Method: ``Z_detail[i,j] = A_detail[i,j] · q_detail[j]`` (commodity i
    used to make commodity j, in $). Group rows and columns by their BEA
    summary parent, sum, and divide by aggregated ``q`` to get summary A
    coefficients. This is the same construction BEA uses to derive
    summary A from a detail Use table.
    """
    common_idx = a_detail.index.intersection(q_detail.index)
    a_aligned = a_detail.loc[common_idx, common_idx]
    q_aligned = q_detail.reindex(common_idx).fillna(0.0)

    z = a_aligned.multiply(q_aligned.to_numpy(), axis=1)
    cs_to_sum = {k: v for k, v in cs_to_summary.items() if k in common_idx}
    detail_to_summary_index = pd.Index(
        [cs_to_sum.get(c, "UNMAPPED") for c in common_idx]
    )

    z.index = detail_to_summary_index
    z.columns = detail_to_summary_index
    z_summary = z.groupby(level=0).sum().T.groupby(level=0).sum().T

    q_summary = pd.Series(q_aligned.to_numpy(), index=detail_to_summary_index)
    q_summary = q_summary.groupby(level=0).sum()

    safe_q = q_summary.replace(0, np.nan)
    a_summary = z_summary.divide(safe_q, axis=1)
    a_summary = a_summary.fillna(0.0)
    a_summary = a_summary.drop(index="UNMAPPED", errors="ignore").drop(
        columns="UNMAPPED", errors="ignore"
    )
    return a_summary


def _bea_observed_summary_a(year: int) -> pd.DataFrame:
    """BEA observed summary A = ``A_dom_summary + A_imp_summary`` at year."""
    return derive_summary_Adom_usa(year).add(
        derive_summary_Aimp_usa(year), fill_value=0.0
    )


def _cell_errors_one_pair(
    a_pred: pd.DataFrame, a_obs: pd.DataFrame
) -> tuple[float, float, list[tuple[str, str, float]]]:
    """Per (approach, year) cell-level summary stats.

    Both inputs are summary-level A. Returns:
    - Z-magnitude-weighted RMSE (cells with bigger flows dominate)
    - mean absolute diff (uniform-weighted)
    - top-K worst cells by signed difference
    """
    common_rows = a_pred.index.intersection(a_obs.index)
    common_cols = a_pred.columns.intersection(a_obs.columns)
    pred = a_pred.loc[common_rows, common_cols].to_numpy()
    obs = a_obs.loc[common_rows, common_cols].to_numpy()
    diff = pred - obs

    abs_obs = np.abs(obs)
    weight_sum = abs_obs.sum()
    weights = (
        abs_obs / weight_sum if weight_sum > 0 else np.ones_like(abs_obs) / abs_obs.size
    )
    rmse = float(np.sqrt(np.sum(weights * diff**2)))
    mean_abs = float(np.abs(diff).mean())

    flat = diff.flatten()
    flat_abs = np.abs(flat)
    worst = np.argsort(flat_abs)[::-1][:TOP_K_WORST_CELLS_REPORTED]
    rows_arr = np.asarray(common_rows)
    cols_arr = np.asarray(common_cols)
    n_cols = len(common_cols)
    worst_cells = [
        (str(rows_arr[i // n_cols]), str(cols_arr[i % n_cols]), float(flat[i]))
        for i in worst
    ]
    return rmse, mean_abs, worst_cells


def compute_errors_table() -> pd.DataFrame:
    """Per (approach, year) cell-level errors at summary aggregation."""
    cs_to_summary = _cornerstone_to_summary()
    rows: list[dict[str, object]] = []
    for year in SUMMARY_A_ERROR_YEARS:
        try:
            a_summary_obs = _bea_observed_summary_a(year)
        except Exception as e:  # noqa: BLE001
            logger.warning("BEA summary A unavailable for year=%d (%s)", year, e)
            continue
        for approach in APPROACH_ORDER:
            # Skip year/approach combos the approach doesn't cover. Avoids
            # log noise for known gaps (e.g. useeio_nowcast has no 2024).
            coverage = APPROACH_YEAR_COVERAGE.get(approach)
            if coverage is not None and year not in coverage:
                continue
            a_path = RESULTS_DIR / f"A_{approach}_{year}.parquet"
            q_path = RESULTS_DIR / f"q_{approach}_{year}.parquet"
            if not (a_path.exists() and q_path.exists()):
                logger.warning("Missing %s or %s — skipping", a_path, q_path)
                continue
            a_detail = _load_a_total_dom_plus_imp(approach, year)
            q_detail = _load_q_detail(approach, year)
            a_summary_pred = aggregate_detail_a_to_summary(
                a_detail, q_detail, cs_to_summary
            )
            rmse, mean_abs, worst_cells = _cell_errors_one_pair(
                a_summary_pred, a_summary_obs
            )
            rows.append(
                {
                    "approach": approach,
                    "year": year,
                    "rmse_vs_bea_summary_a": rmse,
                    "mean_abs_diff": mean_abs,
                    "n_summary_cells": int(
                        a_summary_pred.shape[0] * a_summary_pred.shape[1]
                    ),
                    "top_5_worst_cells": "; ".join(
                        f"{r}->{c}:{d:+.3f}" for r, c, d in worst_cells
                    ),
                }
            )
    return pd.DataFrame(rows)


def plot_rmse_ranking(errors_df: pd.DataFrame, path: Path) -> None:
    """Grouped bar chart: x = year, 5 bars per group (one per approach).

    y = Z-magnitude-weighted cell-level RMSE between predicted summary A
    (Cornerstone aggregated) and observed summary A (BEA). Tests A's full
    cell shape at summary aggregation, not just column sums.
    """
    pivot = errors_df.pivot_table(
        index="year", columns="approach", values="rmse_vs_bea_summary_a"
    ).reindex(columns=APPROACH_ORDER)

    n_years = len(pivot.index)
    n_approaches = len(APPROACH_ORDER)
    bar_w = 0.8 / n_approaches
    x = np.arange(n_years)

    fig, ax = plt.subplots(figsize=(1.4 * n_years + 2, 5))
    fig.suptitle(
        "Cell-level RMSE of Cornerstone A vs BEA observed summary A — by approach",
        fontsize=11,
    )

    for i, approach in enumerate(APPROACH_ORDER):
        if approach not in pivot.columns:
            continue
        offset = (i - (n_approaches - 1) / 2) * bar_w
        vals = pivot[approach].to_numpy()
        ax.bar(
            x + offset,
            vals,
            width=bar_w,
            color=APPROACH_COLORS[approach],
            label=approach,
            edgecolor="white",
            linewidth=0.4,
        )

    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in pivot.index])
    ax.set_xlabel("year")
    ax.set_ylabel("Z-weighted RMSE")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=9, framealpha=0.4)

    fig.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Computing summary-A cell-level errors across approaches × years")
    errors_df = compute_errors_table()
    errors_df.to_csv(RESULTS_DIR / "summary_a_errors.csv", index=False)

    plot_rmse_ranking(errors_df, PLOTS_DIR / "summary_a_rmse_ranking.png")

    publish_tabs({"summary_a_errors": errors_df})
    logger.info("Step 5 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
