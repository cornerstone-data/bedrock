"""EF and BLy analysis plots from diagnostics Google Sheets.

Reads the ``N_and_diffs``, ``D_and_diffs``, ``D_and_N_significant_sectors``,
and (when present) ``BLy_new_vs_BLy_old`` tabs and renders:

- ``ef_perc_diff_histogram.png``     — 2×2 N/D percent-diff distributions
                                       (all sectors + significant sectors)
- ``ef_n_perc_diff_histogram.png``   — standalone N percent-diff distribution
- ``ef_pct_change_vs_abs_change.png`` — |% change| vs |absolute change|
- ``ef_pct_change_vs_ef_size.png``    — |% change| vs old EF size
- ``ef_abs_change_histogram.png``     — distribution of absolute EF changes
- ``bly_sector_stacked_net_change.png`` — stacked sector contributions to BLy net change
  (only when the BLy tab exists; otherwise omitted, no error)

Usage:
    uv run python -m bedrock.utils.validation.analysis.diagnostics_plots \\
        --sheet-id <google_sheet_id> [--refresh] [--tag <label>] [--out-dir <path>] \\
        [--bly-group-small-threshold <Mt CO2e>]
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Hashable

import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure

from ._cli import common_options, resolve_output_dir, resolve_sheet_id
from .bly_plots import TAB_BLY, bly_plot_options, build_sector_stack_frame
from .fetch import load_tab, load_tabs_optional
from .plotting import (
    DEFAULT_XLIM,
    LEGEND_FONTSIZE,
    TEXT_BOX_FONTSIZE,
    TITLE_FONTSIZE,
    abs_change_histogram,
    apply_axis_fonts,
    dodge_annotations,
    percent_histogram,
    plot_stacked_net_change,
    save_and_close,
    setup_mpl,
)

TAB_N = "N_and_diffs"
TAB_D = "D_and_diffs"
TAB_SIG = "D_and_N_significant_sectors"

OUTLIER_COLOR = "#d32f2f"
POINT_COLOR = "#546e7a"
SINGLE_PANEL_FIGSIZE = (14, 10)
BLY_FIGSIZE_WIDTH = 5.0
BLY_FIGSIZE_MIN_HEIGHT = 7.0
BLY_FIGSIZE_PER_ROW_HEIGHT = 0.3


def bly_figsize(max_sectors: int) -> tuple[float, float]:
    """Scale BLy figure height from the ``max_sectors`` ceiling (+ 2 Other rows).

    Using the ceiling — not the realized row count — makes the figure size
    deterministic from the CLI flag, so any two baselines rendered with the
    same ``--bly-max-sectors`` share dimensions.
    """
    ceiling_rows = max_sectors + 2 if max_sectors > 0 else 20
    height = max(
        BLY_FIGSIZE_MIN_HEIGHT, 4.0 + BLY_FIGSIZE_PER_ROW_HEIGHT * ceiling_rows
    )
    return (BLY_FIGSIZE_WIDTH, height)


def _add_outlier_box(ax: Any, text: str) -> Any:
    """Anchor a left-aligned, translucent outlier box in the upper-right corner.

    Matches the abs-change histogram's outlier-box styling so all plots share
    a consistent look — wheat background at low opacity, monospaced,
    left-multialigned lines. Returns the artist so callers can pass it as an
    ``avoid`` region to ``dodge_annotations``.
    """
    return ax.text(
        0.98,
        0.98,
        text,
        transform=ax.transAxes,
        fontsize=TEXT_BOX_FONTSIZE,
        verticalalignment="top",
        horizontalalignment="right",
        multialignment="left",
        fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="wheat", alpha=0.35),
    )


def _sector_label(sector: Any, sector_name: str, width: int = 20) -> str:
    name = (
        sector_name if len(sector_name) <= width else sector_name[: width - 3] + "..."
    )
    return f"{sector} ({name})"


def _drop_old_only(df: pd.DataFrame) -> pd.DataFrame:
    if "comparison_type" in df.columns:
        return df[~df["comparison_type"].astype(str).str.startswith("old-only")]
    return df


def _normalize_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Canonicalize the sector-code column name across sheet variants.

    Some tabs (e.g. USEEIO) use ``index`` as the sector-code column instead
    of ``sector``. Rename so downstream code can assume ``sector`` exists.
    """
    if "sector" not in df.columns and "index" in df.columns:
        df = df.rename(columns={"index": "sector"})
    return df


def _normalize_perc_diff(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Normalize a ``<prefix>_perc_diff`` column to numeric fractions.

    Tolerates the legacy ``<prefix>_no_manual_adj_perc_diff`` column
    (percent-string) by routing it to the canonical ``<prefix>_perc_diff``.
    """
    target = f"{prefix}_perc_diff"
    source = (
        f"{prefix}_no_manual_adj_perc_diff"
        if f"{prefix}_no_manual_adj_perc_diff" in df.columns
        else target
    )
    out = df.copy()
    out[target] = out[source].map(_parse_pct)
    return out


# ---------- N/D percent-diff histogram ----------


def _format_sector_line(row: "pd.Series[Any]", val_col: str) -> str:
    name = str(row.get("sector_name", "") or "")
    sector = str(row.get("sector", "") or "")
    return f"{_sector_label(sector, name)}: {float(row[val_col]) * 100:+.1f}%"


_BEYOND_20_MAX_SHOWN = 30


def _beyond_20_text(
    df: pd.DataFrame,
    filter_col: str,
    *,
    show_col: str | None = None,
    label: str = "Beyond ±20%",
) -> str:
    """Render a ``Beyond ±20%`` summary, capped at ``_BEYOND_20_MAX_SHOWN`` rows.

    Filters by ``|df[filter_col]| > 0.20``, sorts by ``|filter_col|``
    descending, and shows ``show_col`` (defaults to ``filter_col``) for each
    row. The header reports the true total when the list is truncated.
    """
    show = show_col or filter_col
    beyond = df[df[filter_col].abs() > 0.20].copy()
    if beyond.empty:
        return f"{label}: None"
    beyond = beyond.sort_values(filter_col, key=lambda s: s.abs(), ascending=False)
    shown = beyond.head(_BEYOND_20_MAX_SHOWN)
    lines = [_format_sector_line(row, show) for _, row in shown.iterrows()]
    total = len(beyond)
    header = (
        f"{label} ({total}):"
        if total <= _BEYOND_20_MAX_SHOWN
        else f"{label} ({total} total; top {_BEYOND_20_MAX_SHOWN} by |%| shown):"
    )
    return header + "\n" + "\n".join(lines)


def plot_ef_perc_diff_histogram(
    df_n: pd.DataFrame,
    df_d: pd.DataFrame,
    df_sig: pd.DataFrame | None = None,
) -> Figure:
    """N/D percent-diff histograms. 2×2 when ``df_sig`` is given, else 1×2."""
    n_y = "N_perc_diff"
    d_y = "D_perc_diff"

    df_n = _normalize_perc_diff(df_n, "N")
    df_d = _normalize_perc_diff(df_d, "D")
    if df_sig is not None:
        df_sig = _normalize_perc_diff(_normalize_perc_diff(df_sig, "N"), "D")

    df_merged = df_n[["sector", "sector_name", n_y]].merge(
        df_d[["sector", d_y]], on="sector", how="inner"
    )
    d_text_all = _beyond_20_text(
        df_merged, n_y, show_col=d_y, label="N beyond ±20% — D"
    )

    panels: list[tuple[int, int, pd.DataFrame, str, str, str | None]] = [
        (0, 0, df_n, n_y, "N distribution: all sectors", None),
        (0, 1, df_d, d_y, "D distribution: all sectors", d_text_all),
    ]
    n_rows = 1
    if df_sig is not None:
        d_text_sig = _beyond_20_text(
            df_sig, n_y, show_col=d_y, label="N beyond ±20% — D"
        )
        panels.extend(
            [
                (1, 0, df_sig, n_y, "N distribution: significant sectors", None),
                (1, 1, df_sig, d_y, "D distribution: significant sectors", d_text_sig),
            ]
        )
        n_rows = 2

    fig, axes_raw = plt.subplots(n_rows, 2, figsize=(18, 7 * n_rows), squeeze=False)
    axes = axes_raw
    for r, c, df, y_col, title, text_override in panels:
        text = (
            text_override if text_override is not None else _beyond_20_text(df, y_col)
        )
        percent_histogram(
            axes[r, c],
            df[y_col].dropna() * 100,
            xlim=DEFAULT_XLIM,
            xlabel="Percentage Diff (%)",
            ylabel="Count",
            title=title,
            text_box=text,
            text_box_fontsize=TEXT_BOX_FONTSIZE,
            legend_fontsize=LEGEND_FONTSIZE,
        )
        axes[r, c].title.set_fontsize(TITLE_FONTSIZE)
        apply_axis_fonts(axes[r, c])
    ylim_max = max(axes[r, c].get_ylim()[1] for r in range(n_rows) for c in range(2))
    for r in range(n_rows):
        for c in range(2):
            axes[r, c].set_ylim(0, ylim_max)
    fig.tight_layout()
    return fig


def plot_n_perc_diff_histogram(df_n: pd.DataFrame) -> Figure:
    """Standalone histogram of N percent-diff across all sectors."""
    df_n = _normalize_perc_diff(df_n, "N")
    y_col = "N_perc_diff"
    text = _beyond_20_text(df_n, y_col)

    fig, ax = plt.subplots(figsize=SINGLE_PANEL_FIGSIZE)
    percent_histogram(
        ax,
        df_n[y_col].dropna() * 100,
        xlim=DEFAULT_XLIM,
        xlabel="Percentage Diff (%)",
        ylabel="Count",
        title="Distribution of Percentage (%) EF Changes",
        text_box=text,
        text_box_fontsize=11,
        legend_fontsize=13,
    )
    ax.title.set_fontsize(TITLE_FONTSIZE)
    apply_axis_fonts(ax)
    fig.tight_layout()
    return fig


# ---------- EF comparison table + scatter/abs-change plots ----------


def _parse_pct(v: Any) -> float:
    """Parse a percent cell that may be a float fraction or a ``"X%"`` string."""
    if pd.isna(v):
        return float("nan")
    if isinstance(v, str):
        s = v.strip().rstrip("%")
        try:
            return float(s) / 100.0
        except ValueError:
            return float("nan")
    return float(v)


def build_ef_comparison(df_n: pd.DataFrame) -> pd.DataFrame:
    """Reshape ``N_and_diffs`` into a sector-indexed EF comparison table.

    Tolerates the legacy ``N_no_manual_adj_*`` column variant. Returns
    columns: ``ef_new``, ``ef_old``, ``ef_change``, ``ef_pct_change``,
    ``sector_name``. ``df_n`` should already have old-only rows dropped.
    """
    if "N_no_manual_adj_perc_diff" in df_n.columns:
        old_col = "N_no_manual_adj_old_inflated"
        pct_col = "N_no_manual_adj_perc_diff"
    else:
        old_col = "N_old_inflated"
        pct_col = "N_perc_diff"

    sector_key = df_n["sector"].astype("string").fillna(df_n["sector_name"].astype(str))
    out = pd.DataFrame(
        {
            "ef_new": df_n["N_new"].astype(float).to_numpy(),
            "ef_old": df_n[old_col].astype(float).to_numpy(),
            "ef_pct_change": df_n[pct_col].map(_parse_pct).to_numpy(),
            "sector_name": df_n.get("sector_name", sector_key).astype(str).to_numpy(),
        },
        index=pd.Index(sector_key.astype(str), name="sector"),
    )
    out["ef_change"] = out["ef_new"] - out["ef_old"]
    out["ef_pct_change"] = out["ef_pct_change"].replace([np.inf, -np.inf], np.nan)
    return out


def plot_ef_pct_change_vs_abs_change(
    ef_comparison: pd.DataFrame,
    *,
    top_n_labels: int = 8,
    xlim_cap: float = 1.0,
    ylim_cap: float = 100.0,
) -> Figure:
    """Scatter: |absolute EF change| vs |% EF change|.

    Answers: do big % changes correspond to big or small absolute changes?
    """
    sd = ef_comparison.dropna(subset=["ef_pct_change"]).copy()
    sd["_abs_change"] = sd["ef_change"].abs()
    sd["_abs_pct"] = sd["ef_pct_change"].abs()

    fig, ax = plt.subplots(figsize=SINGLE_PANEL_FIGSIZE)
    ax.scatter(
        sd["_abs_change"],
        sd["_abs_pct"] * 100,
        s=40,
        c=POINT_COLOR,
        alpha=0.6,
        edgecolors="k",
        linewidths=0.4,
    )

    on_chart = sd[(sd["_abs_change"] <= xlim_cap) & (sd["_abs_pct"] * 100 <= ylim_cap)]
    outliers = sd[(sd["_abs_change"] > xlim_cap) | (sd["_abs_pct"] * 100 > ylim_cap)]

    to_label_idx = set(on_chart.nlargest(top_n_labels, "_abs_pct").index) | set(
        on_chart.nlargest(top_n_labels, "_abs_change").index
    )
    label_items = [
        (
            float(row["_abs_change"]),
            float(row["_abs_pct"]) * 100,
            _sector_label(sector, str(row["sector_name"])),
        )
        for sector, row in on_chart.loc[list(to_label_idx)].iterrows()
    ]

    outlier_artist = None
    if not outliers.empty:
        lines = [f"Outliers ({len(outliers)} off-chart):"]
        for sector, row in outliers.sort_values("_abs_pct", ascending=False).iterrows():
            lines.append(
                f"  {_sector_label(sector, str(row['sector_name']), width=25)}:"
                f" |Δ|={row['_abs_change']:.2f}, |%|={row['_abs_pct'] * 100:.0f}%"
            )
        outlier_artist = _add_outlier_box(ax, "\n".join(lines))

    ax.set_xlim(0, xlim_cap)
    ax.set_ylim(0, ylim_cap * 1.08)
    ax.set_xlabel("|EF Absolute Change| (kgCO2e/$)")
    ax.set_ylabel("|EF % Change|")
    ax.set_title("EF % Change vs EF Absolute Change", fontsize=TITLE_FONTSIZE, pad=12)
    apply_axis_fonts(ax)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    dodge_annotations(
        ax, label_items, avoid=[outlier_artist] if outlier_artist else None
    )
    return fig


def plot_ef_pct_change_vs_ef_size(
    ef_comparison: pd.DataFrame,
    *,
    top_n_labels: int = 8,
    ylim_cap: float = 100.0,
    pct_threshold: float = 10.0,
) -> Figure:
    """Scatter: old EF size vs |% EF change|.

    Answers: do big % changes happen on large or small EFs?
    """
    sd = ef_comparison.dropna(subset=["ef_pct_change"]).copy()
    sd["_abs_change"] = sd["ef_change"].abs()
    sd["_abs_pct"] = sd["ef_pct_change"].abs()

    fig, ax = plt.subplots(figsize=SINGLE_PANEL_FIGSIZE)
    ax.scatter(
        sd["ef_old"],
        sd["_abs_pct"] * 100,
        s=40,
        c=POINT_COLOR,
        alpha=0.6,
        edgecolors="k",
        linewidths=0.4,
    )

    on_chart = sd[sd["_abs_pct"] * 100 <= ylim_cap]
    outliers = sd[sd["_abs_pct"] * 100 > ylim_cap]

    to_label_idx = set(on_chart.nlargest(top_n_labels, "_abs_pct").index) | set(
        on_chart.nlargest(top_n_labels, "_abs_change").index
    )
    label_items = [
        (
            float(row["ef_old"]),
            float(row["_abs_pct"]) * 100,
            _sector_label(sector, str(row["sector_name"])),
        )
        for sector, row in on_chart.loc[list(to_label_idx)].iterrows()
    ]

    outlier_artist = None
    if not outliers.empty:
        lines = [f"Outliers ({len(outliers)} off-chart):"]
        for sector, row in outliers.sort_values("_abs_pct", ascending=False).iterrows():
            lines.append(
                f"  {_sector_label(sector, str(row['sector_name']), width=25)}:"
                f" EF={row['ef_old']:.2f}, |%|={row['_abs_pct'] * 100:.0f}%"
            )
        outlier_artist = _add_outlier_box(ax, "\n".join(lines))

    median_ef = float(sd["ef_old"].median())
    ax.axvline(median_ef, color=OUTLIER_COLOR, linestyle="--", linewidth=1.5, alpha=0.7)
    ax.axhline(
        pct_threshold, color=OUTLIER_COLOR, linestyle="--", linewidth=1.5, alpha=0.7
    )
    label_bbox = dict(
        boxstyle="round,pad=0.3",
        facecolor="white",
        edgecolor=OUTLIER_COLOR,
        alpha=0.9,
    )
    ax.text(
        median_ef,
        -0.04,
        f"median EF: {median_ef:.2f}",
        transform=ax.get_xaxis_transform(),
        color=OUTLIER_COLOR,
        fontsize=14,
        fontweight="bold",
        ha="center",
        va="top",
        bbox=label_bbox,
    )
    ax.text(
        -0.01,
        pct_threshold,
        f"{pct_threshold:g}%",
        transform=ax.get_yaxis_transform(),
        color=OUTLIER_COLOR,
        fontsize=14,
        fontweight="bold",
        ha="right",
        va="center",
        bbox=label_bbox,
    )

    ax.set_ylim(0, ylim_cap * 1.08)
    ax.set_xlabel("EF Size (old EF, kgCO2e/$)")
    ax.set_ylabel("|EF % Change|")
    ax.set_title("EF % Change vs EF Size", fontsize=TITLE_FONTSIZE, pad=12)
    apply_axis_fonts(ax)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    dodge_annotations(
        ax, label_items, avoid=[outlier_artist] if outlier_artist else None
    )
    return fig


def plot_ef_abs_change_histogram(
    ef_comparison: pd.DataFrame,
    *,
    n_bins: int = 50,
    clip_range: tuple[float, float] = (-0.1, 0.1),
) -> Figure:
    """Histogram of absolute EF changes (kgCO2e/$) across all sectors."""
    sd = ef_comparison.dropna(subset=["ef_change"])
    changes = sd["ef_change"]
    sector_name = sd["sector_name"]

    def outlier_label(idx: Hashable, v: float) -> str:
        desc = str(sector_name.get(idx, idx))[:30]
        return f"  {idx} ({desc}): {v:+.4f}"

    fig, ax = plt.subplots(figsize=SINGLE_PANEL_FIGSIZE)
    abs_change_histogram(
        ax,
        changes,
        clip_range=clip_range,
        n_bins=n_bins,
        outlier_label_fn=outlier_label,
        xlabel="EF Absolute Change (kgCO2e/$)",
        ylabel="Number of Sectors",
        title="Distribution of Absolute EF Changes",
        legend_fontsize=LEGEND_FONTSIZE,
        outlier_fontsize=TEXT_BOX_FONTSIZE,
    )
    ax.set_title("Distribution of Absolute EF Changes", fontsize=TITLE_FONTSIZE)
    apply_axis_fonts(ax)
    fig.tight_layout()
    return fig


# ---------- entry point ----------


def plot(
    sheet_id: str,
    out_dir: Path,
    *,
    refresh: bool,
    bly_group_small_threshold: float,
    bly_max_sectors: int,
) -> None:
    df_n = _drop_old_only(_normalize_schema(load_tab(sheet_id, TAB_N, refresh=refresh)))
    df_d = _drop_old_only(_normalize_schema(load_tab(sheet_id, TAB_D, refresh=refresh)))
    sig_raw = load_tabs_optional(sheet_id, [TAB_SIG], refresh=refresh)[TAB_SIG]
    df_sig = _drop_old_only(_normalize_schema(sig_raw)) if sig_raw is not None else None

    fig_h = plot_ef_perc_diff_histogram(df_n, df_d, df_sig)
    save_and_close(fig_h, out_dir / "ef_perc_diff_histogram.png")

    fig_n = plot_n_perc_diff_histogram(df_n)
    save_and_close(fig_n, out_dir / "ef_n_perc_diff_histogram.png")

    ef_comparison = build_ef_comparison(df_n)

    fig_abs = plot_ef_pct_change_vs_abs_change(ef_comparison)
    save_and_close(fig_abs, out_dir / "ef_pct_change_vs_abs_change.png")

    fig_size = plot_ef_pct_change_vs_ef_size(ef_comparison)
    save_and_close(fig_size, out_dir / "ef_pct_change_vs_ef_size.png")

    fig_hist = plot_ef_abs_change_histogram(ef_comparison)
    save_and_close(fig_hist, out_dir / "ef_abs_change_histogram.png")

    # Optional NAB tab: missing or unreadable tabs must not fail the suite.
    bly_raw = load_tabs_optional(sheet_id, [TAB_BLY], refresh=refresh)[TAB_BLY]
    if bly_raw is not None:
        bly_frame = build_sector_stack_frame(
            bly_raw,
            group_small_threshold=bly_group_small_threshold,
            max_sectors=bly_max_sectors,
        )
        fig_bly, ax_bly = plt.subplots(figsize=bly_figsize(bly_max_sectors))
        plot_stacked_net_change(
            ax_bly,
            bly_frame,
            title="Sector contributions to net change (BLy)",
            ylabel="Gross change (MMT CO2e)",
        )
        fig_bly.tight_layout()
        save_and_close(fig_bly, out_dir / "bly_sector_stacked_net_change.png")


@click.command()
@common_options
@bly_plot_options
def main(
    baseline: str | None,
    sheet_id: str | None,
    refresh: bool,
    tag: str | None,
    out_dir: Path | None,
    bly_group_small_threshold: float,
    bly_max_sectors: int,
) -> None:
    resolved_sheet_id = resolve_sheet_id(sheet_id, baseline)
    setup_mpl(font_size=14)
    _, out = resolve_output_dir(resolved_sheet_id, tag, out_dir, baseline=baseline)
    plot(
        resolved_sheet_id,
        out,
        refresh=refresh,
        bly_group_small_threshold=bly_group_small_threshold,
        bly_max_sectors=bly_max_sectors,
    )


if __name__ == "__main__":
    main()
