"""Step 2.5 of epic #337: cell-level set stability and persistence diagnostics.

Reads ``A_cells_long.parquet`` from Step 2 and unpacks what the share-of-cells
line chart in Step 2 aggregates away. Two questions:

- **Set stability**: are the cells above a divergence threshold the *same*
  cells year-over-year (structural offset), or is membership rotating
  (transient drift averaging to a constant share)?
- **Persistence**: of the cells that *ever* disagree, how is the disagreement
  distributed across "always" vs "occasional" — i.e. what fraction of the
  ever-above-threshold population is consistently above every year?

Outputs:

- ``set_stability_jaccard_thr{thr}_{kind}.png`` — 3×2 grid of year×year
  Jaccard heatmaps (approach × baseline). Upper-triangle only with
  ``vmin=0.5`` so the meaningful range fills the colorbar; each panel
  carries an off-diagonal-mean tag. High off-diagonal mean ⇒ structural
  offset; low ⇒ rotating membership.
- ``persistence_by_threshold_{kind}.png`` — multi-threshold conditional
  persistence composite. One panel per threshold in ``PLOT_THRESHOLDS``
  (strictest → loosest) plus a shared legend. Bars are baseline-grouped
  (left 3 = vs USEEIO, gap, right 3 = vs CEDA-US) and stacked by
  years-above-threshold buckets. Denominator is **cells ever above
  threshold** so the always-above fraction is directly legible.
  Structural offsets keep their always-share roughly constant as the
  threshold tightens; rotating membership doesn't.
- ``set_stability_jaccard.csv``, ``persistence_categories.csv`` — long-format
  CSVs covering ``ALL_THRESHOLDS``. ``persistence_categories.csv`` carries
  both the absolute ``share`` (denominator = all cells) and
  ``conditional_share`` (denominator = ever-above-threshold cells).
- ``set_stability_jaccard``, ``persistence_categories`` tabs appended to
  the run-report Sheet (sheet ID from ``last_run_sheet_id.txt``).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.derive_A_cells_stability
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.ticker import PercentFormatter

from bedrock.analysis.a_matrix_time_series._run_report import publish_tabs
from bedrock.analysis.a_matrix_time_series.constants import (
    FOCUS_APPROACHES,
    PLOTS_DIR,
    RESULTS_DIR,
)

logger = logging.getLogger(__name__)

A_CELLS_LONG_PATH = RESULTS_DIR / "A_cells_long.parquet"

# Approaches we plot (rows). Reversed from FOCUS_APPROACHES so rows are
# stacked with `useeio_nowcast` at top (external reference reads first),
# then `commodity_price_index`, then `summary_tables` at bottom.
APPROACHES_FOR_STABILITY: tuple[str, ...] = tuple(reversed(FOCUS_APPROACHES))
# (approach_name_in_long, display_label, delta_column_in_long). Carries the
# delta-column suffix so this file is the only consumer; the shared
# `BASELINES` 2-tuple in `constants.py` covers the simpler (name, label) form.
BASELINES_WITH_DELTA_COL: tuple[tuple[str, str, str], ...] = (
    ("useeio", "USEEIO", "delta_vs_useeio"),
    ("ceda_default", "CEDA-US", "delta_vs_ceda"),
)

# CSV/Sheet thresholds — superset of Step 2 (extended up to 1e-1 so the
# economically-meaningful upper range is available downstream).
ALL_THRESHOLDS: tuple[float, ...] = (1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1)
# Subset rendered as plots — heatmaps (one PNG per threshold) and the
# persistence-by-threshold composite (one panel per threshold). Strictest
# first; tighter thresholds (1e-4 and below) are dominated by rounding-noise
# cells and obscure the signal.
PLOT_THRESHOLDS: tuple[float, ...] = (1e-1, 1e-2, 1e-3)

# Color stops in the heatmap — meaningful range is ~0.5..1.0; clamp lower so
# the gradient resolves the high end (where most values live) instead of being
# wasted on the empty 0..0.5 region.
HEATMAP_VMIN: float = 0.5
HEATMAP_VMAX: float = 1.0


# ---------------------------------------------------------------------------
# Core computations
# ---------------------------------------------------------------------------


def _wide_above_threshold(
    long: pd.DataFrame,
    approach: str,
    kind: str,
    delta_col: str,
    threshold: float,
) -> pd.DataFrame:
    """``(cell × year)`` boolean DataFrame: True where ``|delta_col| > threshold``.

    Drops cells with NaN in any year so the per-year sets are over an aligned
    cell population — same filter as ``plot_divergence_share`` in Step 2.

    Consumed by: ``compute_set_stability``, ``compute_persistence_categories``.
    """
    sub = long[(long["approach"] == approach) & (long["dom_or_imp"] == kind)]
    pivot = pd.DataFrame(
        sub.pivot_table(
            index=["row_sector", "col_sector"],
            columns="year",
            values=delta_col,
        ).abs()
    )
    nan_per_row = np.asarray(pivot.isna().any(axis=1))
    return pd.DataFrame(pivot.loc[~nan_per_row] > threshold)


def compute_set_stability(
    long: pd.DataFrame, kind: str, threshold: float
) -> pd.DataFrame:
    """Year×year Jaccard of "above-threshold" cell sets, per (approach, baseline).

    Long-format rows keyed by (approach, baseline, dom_or_imp, threshold,
    year_a, year_b) with set sizes and the Jaccard ratio.
    """
    rows: list[dict[str, object]] = []
    for approach in APPROACHES_FOR_STABILITY:
        for baseline_col, _, delta_col in BASELINES_WITH_DELTA_COL:
            wide = _wide_above_threshold(long, approach, kind, delta_col, threshold)
            if wide.empty:
                continue
            years = sorted(wide.columns)
            arr = wide[years].to_numpy()  # (n_cells, n_years)
            for ai, year_a in enumerate(years):
                set_a = arr[:, ai]
                n_a = int(set_a.sum())
                for bi, year_b in enumerate(years):
                    set_b = arr[:, bi]
                    n_b = int(set_b.sum())
                    intersection = int((set_a & set_b).sum())
                    union = int((set_a | set_b).sum())
                    jac = intersection / union if union > 0 else float("nan")
                    rows.append(
                        {
                            "approach": approach,
                            "baseline": baseline_col,
                            "dom_or_imp": kind,
                            "threshold": threshold,
                            "year_a": int(year_a),
                            "year_b": int(year_b),
                            "n_a": n_a,
                            "n_b": n_b,
                            "n_intersection": intersection,
                            "n_union": union,
                            "jaccard": jac,
                        }
                    )
    return pd.DataFrame(rows)


def compute_persistence_categories(
    long: pd.DataFrame, kind: str, threshold: float
) -> pd.DataFrame:
    """Histogram of per-cell "years above threshold" + conditional share.

    Returns long-format rows per (approach, baseline, dom_or_imp, threshold,
    n_years_above) with:
      - ``n_cells``: number of cells with exactly ``n_years_above`` years above.
      - ``share``: ``n_cells / total cells`` (denominator = aligned cell pop).
      - ``conditional_share``: ``n_cells / cells_ever_above`` (denom excludes
        the ``n_years_above == 0`` bucket). NaN for the ``0`` bucket and when
        no cell is ever above threshold.
      - ``n_ever_above``: count of cells with ``n_years_above >= 1``.
    """
    rows: list[dict[str, object]] = []
    for approach in APPROACHES_FOR_STABILITY:
        for baseline_col, _, delta_col in BASELINES_WITH_DELTA_COL:
            wide = _wide_above_threshold(long, approach, kind, delta_col, threshold)
            if wide.empty:
                continue
            persistence = wide.sum(axis=1).to_numpy().astype(int)
            total = persistence.size
            n_years = wide.shape[1]
            counts = np.bincount(persistence, minlength=n_years + 1)
            n_ever_above = int(total - counts[0])
            for n_years_above, n_cells in enumerate(counts):
                if n_years_above == 0:
                    cond_share = float("nan")
                elif n_ever_above > 0:
                    cond_share = float(n_cells) / n_ever_above
                else:
                    cond_share = float("nan")
                rows.append(
                    {
                        "approach": approach,
                        "baseline": baseline_col,
                        "dom_or_imp": kind,
                        "threshold": threshold,
                        "n_years_above": int(n_years_above),
                        "n_cells": int(n_cells),
                        "share": (
                            float(n_cells) / total if total > 0 else float("nan")
                        ),
                        "conditional_share": cond_share,
                        "n_ever_above": n_ever_above,
                    }
                )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Panel-level plotters (each draws into a provided Axes)
# ---------------------------------------------------------------------------


def _bucket_label(n_years_above: int, max_bucket: int) -> str:
    if n_years_above == 0:
        return "0 (never)"
    if n_years_above == max_bucket:
        return f"{n_years_above} (always)"
    if n_years_above == 1:
        return "1 year"
    return f"{n_years_above} years"


def _draw_jaccard_panel(
    panel_df: pd.DataFrame,
    ax: Axes,
    *,
    title: str,
    show_yticklabels: bool = True,
) -> None:
    """Improved Jaccard heatmap panel: upper triangle only, ``vmin=0.5``,
    annotate off-diagonal cells only, off-diagonal mean tag below the panel.

    Lower triangle is masked because the matrix is symmetric (J(a,b)=J(b,a)).
    Diagonal is suppressed because it is trivially 1 and adds no information.
    """
    if len(panel_df) == 0:
        ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(title, fontsize=10)
        return

    heat = (
        panel_df.pivot(index="year_a", columns="year_b", values="jaccard")
        .sort_index()
        .sort_index(axis=1)
    )
    years = list(heat.columns)
    heat_arr = heat.to_numpy(dtype=float)
    n = len(years)

    display = heat_arr.copy()
    lower_mask = np.tri(n, k=-1, dtype=bool)
    display[lower_mask] = np.nan

    im = ax.imshow(
        display,
        vmin=HEATMAP_VMIN,
        vmax=HEATMAP_VMAX,
        cmap="viridis",
        origin="lower",
        aspect="equal",
    )
    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels([str(y) for y in years], fontsize=8, rotation=45)
    if show_yticklabels:
        ax.set_yticklabels([str(y) for y in years], fontsize=8)
    else:
        ax.set_yticklabels([])

    for ai in range(n):
        for bi in range(ai + 1, n):
            val = float(heat_arr[ai, bi])
            if np.isnan(val):
                continue
            color = "white" if val < (HEATMAP_VMIN + HEATMAP_VMAX) / 2 else "black"
            ax.text(
                bi,
                ai,
                f"{val:.2f}",
                ha="center",
                va="center",
                fontsize=7,
                color=color,
            )

    off_diag_mask = ~np.eye(n, dtype=bool)
    off_vals = heat_arr[off_diag_mask]
    off_vals = off_vals[~np.isnan(off_vals)]
    mean = float(off_vals.mean()) if len(off_vals) > 0 else float("nan")
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(f"off-diag mean = {mean:.2f}", fontsize=9)

    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04)


def _draw_baseline_grouped_persistence(
    panel_sub: pd.DataFrame,
    ax: Axes,
    *,
    threshold: float,
    show_legend: bool = True,
    show_title: bool = True,
) -> tuple[list[Any], list[Any]]:
    """Stacked persistence bars grouped by baseline.

    Bar order is ``[USEEIO group, spacer, CEDA-US group]``. Within each
    group the three approaches appear in ``APPROACHES_FOR_STABILITY`` order.
    A faint dashed vertical separator marks the spacer. Bars stack
    years-above-threshold buckets ``1..N_years``; legend is reordered so
    the "always" bucket sits at the top, matching the visual stack.

    Returns ``(handles, labels)`` for the legend so callers (e.g. the
    dashboard) can build a shared figure-level legend if ``show_legend=False``.
    """
    if len(panel_sub) == 0:
        ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center")
        if show_title:
            ax.set_title(f"|Δ| > {threshold:g}", fontsize=10)
        return [], []

    # Bar order: group by baseline (outer), approach (inner).
    bars: list[tuple[str, str, str]] = []
    for baseline_col, baseline_label, _ in BASELINES_WITH_DELTA_COL:
        for approach in APPROACHES_FOR_STABILITY:
            bars.append((approach, baseline_col, baseline_label))
    bar_labels = [f"{a}\nvs {bl}" for a, _, bl in bars]

    n_per_group = len(APPROACHES_FOR_STABILITY)
    # Position 3 (between groups) is empty — creates the visual spacer.
    x_positions = np.concatenate(
        [
            np.arange(n_per_group, dtype=float),
            np.arange(n_per_group, dtype=float) + n_per_group + 1.0,
        ]
    )

    max_bucket = int(panel_sub["n_years_above"].max())
    cmap = plt.get_cmap("viridis")
    bucket_colors = [
        cmap((k - 1) / max(max_bucket - 1, 1)) for k in range(1, max_bucket + 1)
    ]

    n_ever_per_bar: list[int] = []
    for approach, baseline_col, _ in bars:
        row = panel_sub.loc[
            (panel_sub["approach"] == approach)
            & (panel_sub["baseline"] == baseline_col)
        ]
        n_ever_per_bar.append(int(row["n_ever_above"].iloc[0]) if len(row) > 0 else 0)

    # Min segment height (as fraction of full bar) to qualify for an in-bar
    # n_cells label. Below this, a label would visually collide with neighbors.
    min_label_height = 0.025

    bottom = np.zeros(len(bars))
    for k_idx, n_years_above in enumerate(range(1, max_bucket + 1)):
        heights: list[float] = []
        n_cells_per_bar: list[int] = []
        for approach, baseline_col, _ in bars:
            row = panel_sub.loc[
                (panel_sub["approach"] == approach)
                & (panel_sub["baseline"] == baseline_col)
                & (panel_sub["n_years_above"] == n_years_above)
            ]
            if len(row) > 0:
                cond = float(row["conditional_share"].iloc[0])
                heights.append(0.0 if np.isnan(cond) else cond)
                n_cells_per_bar.append(int(row["n_cells"].iloc[0]))
            else:
                heights.append(0.0)
                n_cells_per_bar.append(0)
        heights_arr = np.asarray(heights, dtype=float)
        ax.bar(
            x_positions,
            heights_arr,
            bottom=bottom,
            color=bucket_colors[k_idx],
            edgecolor="white",
            linewidth=0.4,
            label=_bucket_label(n_years_above, max_bucket),
        )
        # Annotate each segment with its n_cells. White text on dark
        # (low-luminance) segments, black on light. Skip segments too thin to
        # host a label.
        seg_color = bucket_colors[k_idx]
        r, g, b = seg_color[0], seg_color[1], seg_color[2]
        luminance = 0.299 * r + 0.587 * g + 0.114 * b
        text_color = "white" if luminance < 0.55 else "black"
        for ti, (h, n_cells) in enumerate(
            zip(heights_arr, n_cells_per_bar, strict=True)
        ):
            if h < min_label_height or n_cells == 0:
                continue
            center_y = bottom[ti] + h / 2.0
            ax.text(
                x_positions[ti],
                center_y,
                f"{n_cells}",
                ha="center",
                va="center",
                fontsize=8,
                color=text_color,
            )
        bottom = bottom + heights_arr

    ax.set_xticks(x_positions)
    ax.set_xticklabels(bar_labels, fontsize=8, rotation=15, ha="right")
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    ax.set_ylabel("conditional share")
    if show_title:
        ax.set_title(f"|Δ| > {threshold:g}", fontsize=10)
    ax.grid(True, axis="y", alpha=0.3)

    # Dashed separator line between the two baseline groups.
    ax.axvline(x=n_per_group, color="lightgray", lw=0.8, linestyle="--", zorder=0)

    # n_ever_above annotation above each bar — disambiguates "0% always
    # because few cells disagree at all" vs "many cells, none consistent".
    for ti, n_ever in enumerate(n_ever_per_bar):
        ax.text(
            x_positions[ti],
            1.02,
            f"n={n_ever}",
            ha="center",
            va="bottom",
            fontsize=11,
            color="dimgray",
            transform=ax.get_xaxis_transform(),
        )

    handles, labels = ax.get_legend_handles_labels()
    # Reverse so the "always" (yellow) handle sits at the top of the legend,
    # matching the top of the stack.
    handles = handles[::-1]
    labels = labels[::-1]
    if show_legend:
        ax.legend(
            handles,
            labels,
            title="years above threshold",
            loc="center left",
            bbox_to_anchor=(1.02, 0.5),
            fontsize=8,
            framealpha=0.4,
        )
    return handles, labels


# ---------------------------------------------------------------------------
# Top-level plot functions (write a PNG)
# ---------------------------------------------------------------------------


def plot_set_stability_heatmap(
    stability_df: pd.DataFrame, kind: str, threshold: float, path: Path
) -> None:
    """3×2 grid of improved year×year Jaccard heatmaps (approach × baseline)."""
    sub: pd.DataFrame = stability_df.loc[
        (stability_df["dom_or_imp"] == kind) & (stability_df["threshold"] == threshold)
    ]
    n_rows = len(APPROACHES_FOR_STABILITY)
    n_cols = len(BASELINES_WITH_DELTA_COL)
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(5.0 * n_cols, 4.5 * n_rows), squeeze=False
    )
    fig.suptitle(
        f"Year×year Jaccard of cells with |A_approach − A_baseline| > {threshold:g} — {kind}",
        fontsize=12,
    )
    for i, approach in enumerate(APPROACHES_FOR_STABILITY):
        for j, (baseline_col, baseline_label, _) in enumerate(BASELINES_WITH_DELTA_COL):
            panel: pd.DataFrame = sub.loc[
                (sub["approach"] == approach) & (sub["baseline"] == baseline_col)
            ]
            _draw_jaccard_panel(
                panel,
                axes[i][j],
                title=f"{approach} vs {baseline_label}",
            )
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_persistence_by_threshold(
    persistence_df: pd.DataFrame, kind: str, path: Path
) -> None:
    """Multi-threshold conditional-persistence composite for one ``kind``.

    One panel per threshold in ``PLOT_THRESHOLDS`` (strictest → loosest)
    plus a shared legend in the unused grid slot. Each panel uses the
    baseline-grouped bar layout (left 3 = vs USEEIO, gap, right 3 = vs
    CEDA-US). Reading across panels shows whether the always-share
    survives as the threshold tightens — a structural offset stays high;
    rotating membership doesn't.
    """
    n_thr = len(PLOT_THRESHOLDS)
    n_cols = 2
    n_rows = (n_thr + 1 + n_cols - 1) // n_cols  # +1 reserves a slot for the legend
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(8.5 * n_cols, 5.5 * n_rows),
        squeeze=False,
    )
    fig.suptitle(
        f"Conditional cell persistence by threshold — {kind}\n"
        "(denominator = cells ever above threshold)",
        fontsize=13,
    )

    legend_handles: list[Any] = []
    legend_labels: list[Any] = []
    for ti, threshold in enumerate(PLOT_THRESHOLDS):
        r, c = divmod(ti, n_cols)
        ax = axes[r][c]
        panel = persistence_df.loc[
            (persistence_df["dom_or_imp"] == kind)
            & (persistence_df["threshold"] == threshold)
            & (persistence_df["n_years_above"] > 0)
        ]
        handles, labels = _draw_baseline_grouped_persistence(
            panel,
            ax,
            threshold=threshold,
            show_legend=False,
        )
        if handles and not legend_handles:
            legend_handles, legend_labels = handles, labels

    # Hide unused panels and host the shared legend in the first unused slot.
    legend_placed = False
    for ti in range(n_thr, n_rows * n_cols):
        r, c = divmod(ti, n_cols)
        ax = axes[r][c]
        ax.axis("off")
        if not legend_placed and legend_handles:
            ax.legend(
                legend_handles,
                legend_labels,
                title="years above threshold",
                loc="center",
                fontsize=11,
                framealpha=0.4,
            )
            legend_placed = True

    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Sheet publish + main
# ---------------------------------------------------------------------------


def main() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    if not A_CELLS_LONG_PATH.exists():
        raise FileNotFoundError(
            f"{A_CELLS_LONG_PATH} not found. Run derive_A_cells_long first (Step 2)."
        )

    long = pd.read_parquet(A_CELLS_LONG_PATH)
    logger.info("Loaded %s (rows=%d)", A_CELLS_LONG_PATH, len(long))

    stability_chunks: list[pd.DataFrame] = []
    persistence_chunks: list[pd.DataFrame] = []
    for kind in ("dom", "imp"):
        for threshold in ALL_THRESHOLDS:
            stab = compute_set_stability(long, kind, threshold)
            persist = compute_persistence_categories(long, kind, threshold)
            stability_chunks.append(stab)
            persistence_chunks.append(persist)
            if threshold in PLOT_THRESHOLDS:
                plot_set_stability_heatmap(
                    stab,
                    kind,
                    threshold,
                    PLOTS_DIR / f"set_stability_jaccard_thr{threshold:g}_{kind}.png",
                )

    stability_df = pd.concat(stability_chunks, ignore_index=True)
    persistence_df = pd.concat(persistence_chunks, ignore_index=True)
    stability_df.to_csv(RESULTS_DIR / "set_stability_jaccard.csv", index=False)
    persistence_df.to_csv(RESULTS_DIR / "persistence_categories.csv", index=False)

    for kind in ("dom", "imp"):
        plot_persistence_by_threshold(
            persistence_df,
            kind,
            PLOTS_DIR / f"persistence_by_threshold_{kind}.png",
        )

    publish_tabs(
        {
            "set_stability_jaccard": stability_df,
            "persistence_categories": persistence_df,
        }
    )
    logger.info("Step 2.5 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
