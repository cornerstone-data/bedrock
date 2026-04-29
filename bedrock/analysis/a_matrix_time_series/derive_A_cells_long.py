"""Step 2 of epic #337: cell-by-cell time-series diagnostics for the A matrix.

Reads the parquet caches produced by ``derive_A_time_series.py`` (Step 1, #340)
and produces:

- ``A_cells_long.parquet`` — long-format
  ``(row_sector, col_sector, year, approach, dom_or_imp, A_value,
    delta_from_2017, delta_yoy, delta_vs_useeio, delta_vs_ceda)``. ~12.8M rows.
  Two baseline-divergence columns honor the **two-baseline convention** from
  the analysis plan: every comparison is reported against both USEEIO (the
  unchanged BEA-2017 base) and CEDA-US (the production-default approach at
  the same year).

- ``scatter_vs_baselines_{dom,imp}.png`` — 3×2 grid (alternative approach ×
  baseline) of element-wise scatter plots. Per-row target year is the latest
  year where the approach and both baselines all have data:
  ``commodity_price_index`` and ``industry_price_index`` rows resolve to 2024;
  ``summary_tables`` falls back to 2023 (BEA Excel hasn't published 2024 yet).
  All 6 panels share the same ``xlim``/``ylim`` so the ``y=x`` line is a true
  45° reference. Each panel has a top-left summary box with ``n``, mean / p95
  / max ``|Δ|``, and ``R²``.

- ``divergence_share_{dom,imp}.png`` — 3×2 grid (alternative × baseline) of
  the share of cells whose ``|A_approach − A_baseline|`` exceeds each
  threshold (1e-4, 1e-3, 1e-2, 1e-1) plotted over years. Answers "how
  widespread is the divergence and how does it spread?" — complements the
  scatter (which shows magnitude at a single year).

- ``baseline_reference_{dom,imp}.png`` — 1×2 reference plot comparing the
  two baselines directly: USEEIO-vs-CEDA scatter at the latest common year
  plus share-of-cells over thresholds across years. Provides a "reference
  floor" for interpreting alternative-vs-baseline divergence — readers can
  read the alternative-vs-USEEIO and alternative-vs-CEDA panels against
  the gap between the baselines themselves.

- ``divergence_vs_useeio`` and ``divergence_vs_ceda`` tabs appended to the
  run-report Sheet. Each row is per (approach, year, dom_or_imp), with
  ``max``, ``p99``, ``p95``, ``p75``, ``p50``, ``mean``, and
  ``n_above_1pct`` of ``|delta_vs_<baseline>|``. Sheet ID is read from
  ``last_run_sheet_id.txt`` (written by Step 1); if missing, Sheet publish
  is skipped with a warning.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.derive_A_cells_long
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import update_sheet_tab

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"
LAST_RUN_SHEET_ID_PATH = RESULTS_DIR / "last_run_sheet_id.txt"
A_CELLS_LONG_PATH = RESULTS_DIR / "A_cells_long.parquet"

SCATTER_APPROACHES = ("commodity_price_index", "industry_price_index", "summary_tables")
SCATTER_BASELINES = (("useeio", "USEEIO"), ("ceda_default", "CEDA-US"))


def _list_pairs() -> list[tuple[str, int]]:
    """Discover (approach, year) pairs from the parquet cache.

    Skips files that don't match ``A_{approach}_{4-digit-year}.parquet`` so
    other artifacts in the same dir (e.g. ``A_cells_long.parquet``) are
    ignored.
    """
    pairs: list[tuple[str, int]] = []
    for path in sorted(RESULTS_DIR.glob("A_*.parquet")):
        stem = path.stem  # e.g. A_useeio_2017
        body = stem[2:]  # strip leading "A_"
        approach, _, year_str = body.rpartition("_")
        if not (year_str.isdigit() and len(year_str) == 4):
            continue
        pairs.append((approach, int(year_str)))
    return pairs


def _load_pair(approach: str, year: int) -> dict[str, pd.DataFrame]:
    """Load Adom + Aimp from the combined parquet for one (approach, year)."""
    combined = pd.read_parquet(RESULTS_DIR / f"A_{approach}_{year}.parquet")
    return {
        "dom": pd.DataFrame(combined.loc["dom"]),
        "imp": pd.DataFrame(combined.loc["imp"]),
    }


def _melt(df: pd.DataFrame, approach: str, year: int, kind: str) -> pd.DataFrame:
    """Wide A matrix → long (row_sector, col_sector, A_value) + metadata.

    Direct numpy-based melt: faster than ``df.stack().reset_index(...)`` and
    avoids the duplicate-column collision when both axes share the source
    parquet's ``sector`` name.
    """
    rows = df.index.to_numpy()
    cols = df.columns.to_numpy()
    return pd.DataFrame(
        {
            "row_sector": np.repeat(rows, len(cols)),
            "col_sector": np.tile(cols, len(rows)),
            "A_value": df.to_numpy().ravel(),
            "approach": approach,
            "year": year,
            "dom_or_imp": kind,
        }
    )


def build_a_cells_long() -> pd.DataFrame:
    """Concat all cells; attach intra-approach drift (``delta_from_2017``,
    ``delta_yoy``) and dual-baseline divergence (``delta_vs_useeio``,
    ``delta_vs_ceda``) per (approach, dom_or_imp, row_sector, col_sector,
    year)."""
    chunks: list[pd.DataFrame] = []
    for approach, year in _list_pairs():
        matrices = _load_pair(approach, year)
        for kind, mat in matrices.items():
            chunks.append(_melt(mat, approach, year, kind))

    long = pd.concat(chunks, ignore_index=True)

    cell_keys = ["approach", "dom_or_imp", "row_sector", "col_sector"]
    long = long.sort_values(cell_keys + ["year"]).reset_index(drop=True)

    # Intra-approach drift: this approach's value at year y, minus this
    # approach's value at 2017.
    base_2017 = (
        long[long["year"] == 2017]
        .set_index(cell_keys)["A_value"]
        .rename("A_value_2017")
    )
    long = long.join(base_2017, on=cell_keys)
    long["delta_from_2017"] = long["A_value"] - long["A_value_2017"]
    long.drop(columns="A_value_2017", inplace=True)

    long["delta_yoy"] = long.groupby(cell_keys)["A_value"].diff()

    # Cross-approach divergence vs each baseline at the same year.
    same_year_keys = ["year", "dom_or_imp", "row_sector", "col_sector"]
    for baseline_approach, col_name in (
        ("useeio", "delta_vs_useeio"),
        ("ceda_default", "delta_vs_ceda"),
    ):
        baseline = (
            long[long["approach"] == baseline_approach]
            .set_index(same_year_keys)["A_value"]
            .rename(f"A_value_{baseline_approach}")
        )
        long = long.join(baseline, on=same_year_keys)
        long[col_name] = long["A_value"] - long[f"A_value_{baseline_approach}"]
        long.drop(columns=f"A_value_{baseline_approach}", inplace=True)

    return long[
        [
            "approach",
            "dom_or_imp",
            "year",
            "row_sector",
            "col_sector",
            "A_value",
            "delta_from_2017",
            "delta_yoy",
            "delta_vs_useeio",
            "delta_vs_ceda",
        ]
    ]


def compute_divergence_quantiles(long: pd.DataFrame, baseline: str) -> pd.DataFrame:
    """Per (approach, year, dom_or_imp): quantile stats of ``|delta_vs_<baseline>|``.

    ``baseline`` is the suffix on the column, i.e. ``"useeio"`` or ``"ceda"``.
    """
    col = f"delta_vs_{baseline}"
    rows: list[dict[str, object]] = []
    for (approach, year, kind), group in long.groupby(
        ["approach", "year", "dom_or_imp"]
    ):
        abs_delta = group[col].dropna().abs()
        if abs_delta.empty:
            continue
        rows.append(
            {
                "approach": approach,
                "year": int(year),
                "dom_or_imp": kind,
                "n_cells": int(abs_delta.size),
                "max": float(abs_delta.max()),
                "p99": float(abs_delta.quantile(0.99)),
                "p95": float(abs_delta.quantile(0.95)),
                "p75": float(abs_delta.quantile(0.75)),
                "p50": float(abs_delta.quantile(0.50)),
                "mean": float(abs_delta.mean()),
                "n_above_1pct": int((abs_delta > 0.01).sum()),
            }
        )
    return pd.DataFrame(rows)


def _latest_common_year(long: pd.DataFrame, approaches: list[str]) -> int | None:
    """Latest year for which every approach in ``approaches`` has data."""
    years_per_approach = [
        set(long.loc[long["approach"] == a, "year"].unique()) for a in approaches
    ]
    if not years_per_approach:
        return None
    common = set.intersection(*years_per_approach)
    return max(common) if common else None


def plot_scatter_vs_baselines(long: pd.DataFrame, kind: str, path: Path) -> None:
    """3×2 grid of element-wise scatters; rows = alternative approach,
    cols = baseline (USEEIO | CEDA-US).

    Per-row target year: the latest year where the approach AND both
    baselines all have data. ``commodity_price_index`` /
    ``industry_price_index`` rows resolve to 2024; ``summary_tables`` falls
    back to 2023 (BEA Excel hasn't published 2024). The actual year used
    appears in each panel title.

    All 6 panels share the same ``xlim``/``ylim`` (global min/max across
    every panel's data) so the ``y=x`` line is a true 45° reference and the
    panels are visually comparable. Each panel has a top-left summary box
    with ``n``, mean / p95 / max ``|Δ|`` and ``R²``.
    """
    sub = long[long["dom_or_imp"] == kind]

    panel_data: list[
        tuple[int, int, str, str, int, "np.ndarray[Any, Any]", "np.ndarray[Any, Any]"]
    ] = []
    global_min = np.inf
    global_max = -np.inf

    for i, approach in enumerate(SCATTER_APPROACHES):
        target_year = _latest_common_year(
            sub, [approach, *(b[0] for b in SCATTER_BASELINES)]
        )
        if target_year is None:
            logger.warning(
                "No common year between %s and baselines for kind=%s; " "skipping row.",
                approach,
                kind,
            )
            continue

        year_pivot = sub[sub["year"] == target_year].pivot_table(
            index=["row_sector", "col_sector"],
            columns="approach",
            values="A_value",
        )
        for j, (baseline_col, baseline_label) in enumerate(SCATTER_BASELINES):
            if (
                approach not in year_pivot.columns
                or baseline_col not in year_pivot.columns
            ):
                continue
            x = year_pivot[baseline_col].to_numpy()
            y = year_pivot[approach].to_numpy()
            mask = ~(np.isnan(x) | np.isnan(y))
            x = x[mask]
            y = y[mask]
            if x.size == 0:
                continue
            global_min = min(global_min, float(x.min()), float(y.min()))
            global_max = max(global_max, float(x.max()), float(y.max()))
            panel_data.append((i, j, approach, baseline_label, target_year, x, y))

    if not panel_data:
        logger.warning("No scatter panels could be drawn for kind=%s.", kind)
        return

    n_rows = len(SCATTER_APPROACHES)
    n_cols = len(SCATTER_BASELINES)
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(4 * n_cols, 4 * n_rows), squeeze=False
    )
    fig.suptitle(f"A_approach vs A_baseline — {kind}", fontsize=12)

    for ax_row in axes:
        for ax in ax_row:
            ax.axis("off")

    for i, j, approach, baseline_label, target_year, x, y in panel_data:
        ax = axes[i][j]
        ax.axis("on")
        ax.scatter(x, y, s=8, alpha=0.35, color="steelblue", edgecolor="none")
        ax.plot(
            [global_min, global_max],
            [global_min, global_max],
            "r--",
            lw=0.6,
            alpha=0.7,
            label="y=x",
        )
        ax.set_xlim(global_min, global_max)
        ax.set_ylim(global_min, global_max)
        ax.set_aspect("equal", adjustable="box")
        ax.set_xlabel(f"{baseline_label} A_value")
        ax.set_ylabel(f"{approach} A_value")
        ax.set_title(f"{approach} vs {baseline_label} ({target_year})", fontsize=9)
        ax.grid(True, alpha=0.3)

        x_f = np.asarray(x, dtype=float)
        y_f = np.asarray(y, dtype=float)
        abs_delta = np.abs(y_f - x_f)
        r2 = (
            float(np.corrcoef(x_f, y_f)[0, 1] ** 2)
            if x_f.std() > 0 and y_f.std() > 0
            else float("nan")
        )
        stats_text = (
            f"n = {x.size:,}\n"
            f"mean |Δ| = {abs_delta.mean():.4f}\n"
            f"p95 |Δ|  = {np.quantile(abs_delta, 0.95):.4f}\n"
            f"max |Δ|  = {abs_delta.max():.4f}\n"
            f"R²       = {r2:.4f}"
        )
        ax.text(
            0.02,
            0.98,
            stats_text,
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=11,
            family="monospace",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
                "edgecolor": "gray",
            },
        )

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


DIVERGENCE_THRESHOLDS: tuple[float, ...] = (1e-6, 1e-5, 1e-4, 1e-3, 1e-2)


def plot_divergence_share(long: pd.DataFrame, kind: str, path: Path) -> None:
    """3×2 grid: share of cells whose ``|delta_vs_baseline|`` exceeds each
    threshold, plotted over years.

    Rows are the three alternative approaches; columns are baselines
    (USEEIO | CEDA-US). Each panel has one line per threshold in
    ``DIVERGENCE_THRESHOLDS`` (1e-4, 1e-3, 1e-2, 1e-1); y-axis is the
    fraction of cells whose absolute deviation from the baseline at that
    year exceeds the threshold. Answers "how widespread is the divergence,
    and how does it spread over time?" — a complement to
    ``scatter_vs_baselines`` (which shows magnitude at one snapshot).
    """
    sub = long[long["dom_or_imp"] == kind]
    n_rows = len(SCATTER_APPROACHES)
    n_cols = len(SCATTER_BASELINES)
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(6 * n_cols, 4 * n_rows), squeeze=False
    )
    fig.suptitle(
        f"Share of cells with |A_approach − A_baseline| above threshold — {kind}",
        fontsize=12,
    )

    cmap = plt.get_cmap("viridis")
    threshold_colors = [
        cmap(i / max(len(DIVERGENCE_THRESHOLDS) - 1, 1))
        for i in range(len(DIVERGENCE_THRESHOLDS))
    ]

    for i, approach in enumerate(SCATTER_APPROACHES):
        for j, (baseline_col, baseline_label) in enumerate(SCATTER_BASELINES):
            ax = axes[i][j]
            delta_col = (
                "delta_vs_useeio" if baseline_col == "useeio" else "delta_vs_ceda"
            )
            approach_sub = sub[sub["approach"] == approach]
            pivot = approach_sub.pivot_table(
                index=["row_sector", "col_sector"],
                columns="year",
                values=delta_col,
            ).abs()
            years_arr = np.array(sorted(pivot.columns), dtype=float)
            values_arr = pivot.to_numpy()
            keep = ~np.isnan(values_arr).any(axis=1)
            kept_values = values_arr[keep]
            if kept_values.size == 0:
                ax.text(
                    0.5,
                    0.5,
                    "no data",
                    transform=ax.transAxes,
                    ha="center",
                    va="center",
                )
                continue

            panel_max_share = 0.0
            for thr, color in zip(DIVERGENCE_THRESHOLDS, threshold_colors, strict=True):
                share = (kept_values > thr).mean(axis=0)
                panel_max_share = max(panel_max_share, float(share.max()))
                ax.plot(
                    years_arr,
                    share,
                    color=color,
                    lw=1.8,
                    marker="o",
                    markersize=3,
                    label=f"|Δ| > {thr:g}",
                )

            ax.set_xlim(years_arr.min(), years_arr.max())
            ax.set_ylim(0, max(panel_max_share * 1.1, 0.01))
            ax.set_xlabel("year")
            ax.set_ylabel("share of cells")
            ax.set_title(
                f"{approach} vs {baseline_label} (n={int(keep.sum())} cells)",
                fontsize=10,
            )
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper left", fontsize=8, framealpha=0.85)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _publish_divergence_tabs(
    div_useeio_df: pd.DataFrame, div_ceda_df: pd.DataFrame
) -> None:
    """Append divergence summary tabs to the run-report Sheet, if available."""
    if not LAST_RUN_SHEET_ID_PATH.exists():
        logger.warning(
            "No %s found — skipping Sheet publish. Run derive_A_time_series "
            "first (with valid Drive auth) to create the run report.",
            LAST_RUN_SHEET_ID_PATH,
        )
        return
    sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
    try:
        update_sheet_tab(sheet_id, "divergence_vs_useeio", div_useeio_df)
        update_sheet_tab(sheet_id, "divergence_vs_ceda", div_ceda_df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local artifacts still complete.",
            type(e).__name__,
            e,
        )
        return
    logger.info("Updated tabs on sheet %s", sheet_id)


def plot_baseline_reference(long: pd.DataFrame, kind: str, path: Path) -> None:
    """1×2 reference figure comparing the two baselines (USEEIO vs CEDA-US).

    Left: scatter of A_useeio (x) vs A_ceda_default (y) at the latest year
    both baselines have data, square aspect, shared limits, with summary
    stats (n, mean / p95 / max ``|Δ|``, R²).

    Right: share of cells with ``|A_useeio − A_ceda_default|`` above each
    threshold in ``DIVERGENCE_THRESHOLDS``, plotted over years.

    Provides a "reference floor": divergence between alternative approaches
    and a baseline can be read against the baseline-vs-baseline divergence
    from the same data.
    """
    sub = long[long["dom_or_imp"] == kind]
    target_year = _latest_common_year(sub, ["useeio", "ceda_default"])
    if target_year is None:
        logger.warning(
            "No common year for useeio + ceda_default, kind=%s; skipping baseline ref.",
            kind,
        )
        return

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), squeeze=False)
    fig.suptitle(f"Baseline reference: USEEIO vs CEDA-US — {kind}", fontsize=12)

    # --- Left: scatter at latest year --------------------------------------
    ax_scatter = axes[0][0]
    year_pivot = sub[sub["year"] == target_year].pivot_table(
        index=["row_sector", "col_sector"],
        columns="approach",
        values="A_value",
    )
    if "useeio" in year_pivot.columns and "ceda_default" in year_pivot.columns:
        x = year_pivot["useeio"].to_numpy()
        y = year_pivot["ceda_default"].to_numpy()
        mask = ~(np.isnan(x) | np.isnan(y))
        x_f = np.asarray(x[mask], dtype=float)
        y_f = np.asarray(y[mask], dtype=float)
        ax_scatter.scatter(
            x_f, y_f, s=8, alpha=0.35, color="darkorange", edgecolor="none"
        )
        lo = float(min(x_f.min(), y_f.min()))
        hi = float(max(x_f.max(), y_f.max()))
        ax_scatter.plot([lo, hi], [lo, hi], "r--", lw=0.6, alpha=0.7, label="y=x")
        ax_scatter.set_xlim(lo, hi)
        ax_scatter.set_ylim(lo, hi)
        ax_scatter.set_aspect("equal", adjustable="box")
        abs_delta = np.abs(y_f - x_f)
        r2 = (
            float(np.corrcoef(x_f, y_f)[0, 1] ** 2)
            if x_f.std() > 0 and y_f.std() > 0
            else float("nan")
        )
        stats_text = (
            f"n = {x_f.size:,}\n"
            f"mean |Δ| = {abs_delta.mean():.4f}\n"
            f"p95 |Δ|  = {np.quantile(abs_delta, 0.95):.4f}\n"
            f"max |Δ|  = {abs_delta.max():.4f}\n"
            f"R²       = {r2:.4f}"
        )
        ax_scatter.text(
            0.02,
            0.98,
            stats_text,
            transform=ax_scatter.transAxes,
            va="top",
            ha="left",
            fontsize=11,
            family="monospace",
            bbox={
                "boxstyle": "round,pad=0.3",
                "facecolor": "white",
                "alpha": 0.85,
                "edgecolor": "gray",
            },
        )
    ax_scatter.set_xlabel("USEEIO A_value")
    ax_scatter.set_ylabel("CEDA-US A_value")
    ax_scatter.set_title(f"USEEIO vs CEDA-US ({target_year})", fontsize=10)
    ax_scatter.grid(True, alpha=0.3)

    # --- Right: share over thresholds, by year ----------------------------
    ax_share = axes[0][1]
    pivot_baseline = sub.pivot_table(
        index=["row_sector", "col_sector"],
        columns=["approach", "year"],
        values="A_value",
    )
    if "useeio" not in pivot_baseline.columns.get_level_values(
        0
    ) or "ceda_default" not in pivot_baseline.columns.get_level_values(0):
        ax_share.text(
            0.5,
            0.5,
            "no baseline data",
            transform=ax_share.transAxes,
            ha="center",
            va="center",
        )
    else:
        useeio_wide = pivot_baseline["useeio"]
        ceda_wide = pivot_baseline["ceda_default"]
        common_years = sorted(set(useeio_wide.columns) & set(ceda_wide.columns))
        if not common_years:
            ax_share.text(
                0.5,
                0.5,
                "no common years",
                transform=ax_share.transAxes,
                ha="center",
                va="center",
            )
        else:
            useeio_arr = useeio_wide[common_years].to_numpy()
            ceda_arr = ceda_wide[common_years].to_numpy()
            abs_delta_arr = np.abs(useeio_arr - ceda_arr)
            keep = ~np.isnan(abs_delta_arr).any(axis=1)
            kept = abs_delta_arr[keep]
            years_arr = np.array(common_years, dtype=float)
            cmap = plt.get_cmap("viridis")
            colors = [
                cmap(i / max(len(DIVERGENCE_THRESHOLDS) - 1, 1))
                for i in range(len(DIVERGENCE_THRESHOLDS))
            ]
            panel_max = 0.0
            for thr, color in zip(DIVERGENCE_THRESHOLDS, colors, strict=True):
                share = (kept > thr).mean(axis=0)
                panel_max = max(panel_max, float(share.max()))
                ax_share.plot(
                    years_arr,
                    share,
                    color=color,
                    lw=1.8,
                    marker="o",
                    markersize=3,
                    label=f"|Δ| > {thr:g}",
                )
            ax_share.set_xlim(years_arr.min(), years_arr.max())
            ax_share.set_ylim(0, max(panel_max * 1.1, 0.01))
            ax_share.legend(loc="upper left", fontsize=8, framealpha=0.85)
    ax_share.set_xlabel("year")
    ax_share.set_ylabel("share of cells")
    ax_share.set_title("Share with |USEEIO − CEDA-US| above threshold", fontsize=10)
    ax_share.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Building A_cells_long.parquet from %s", RESULTS_DIR)
    long = build_a_cells_long()
    long.to_parquet(A_CELLS_LONG_PATH)
    logger.info("Wrote %s (rows=%d)", A_CELLS_LONG_PATH, len(long))

    div_useeio_df = compute_divergence_quantiles(long, "useeio")
    div_ceda_df = compute_divergence_quantiles(long, "ceda")
    div_useeio_df.to_csv(RESULTS_DIR / "divergence_vs_useeio.csv", index=False)
    div_ceda_df.to_csv(RESULTS_DIR / "divergence_vs_ceda.csv", index=False)

    for kind in ("dom", "imp"):
        plot_scatter_vs_baselines(
            long, kind, PLOTS_DIR / f"scatter_vs_baselines_{kind}.png"
        )
        plot_divergence_share(long, kind, PLOTS_DIR / f"divergence_share_{kind}.png")
        plot_baseline_reference(
            long, kind, PLOTS_DIR / f"baseline_reference_{kind}.png"
        )

    _publish_divergence_tabs(div_useeio_df, div_ceda_df)
    logger.info("Step 2 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
