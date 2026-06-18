"""Shared plotting primitives for the diagnostics analysis package.

Centralizes styling (colors, reference lines, text boxes, axis formatters)
and the label-dodge helper so the plot scripts in this package share a
consistent look. Stays close to matplotlib: callers build the figure/axes,
pass in data, and get styled panels back.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Sequence
from pathlib import Path
from typing import Any, Protocol, cast

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure


class _FigureCanvasWithRenderer(Protocol):
    """Canvas backends implement ``get_renderer``; base-class stubs omit it."""

    def get_renderer(self) -> Any: ...


DEFAULT_XLIM: tuple[float, float] = (-115.0, 115.0)
PERCENT_TICKS: tuple[float, ...] = (-100, -50, -20, 0, 20, 50, 100)
DEFAULT_BIN_WIDTH: float = 2.0

HIST_COLOR = "steelblue"
HIST_ALPHA = 0.6
MEDIAN_COLOR = "red"
ZERO_COLOR = "black"
THRESHOLD_COLORS: tuple[str, str] = ("green", "purple")

TITLE_FONTSIZE = 20
AXIS_LABEL_FONTSIZE = 16
TICK_FONTSIZE = 13
TEXT_BOX_FONTSIZE = 11
LEGEND_FONTSIZE = 13


def apply_axis_fonts(ax: Axes) -> None:
    """Apply the shared axis-label and tick font sizes."""
    ax.xaxis.label.set_fontsize(AXIS_LABEL_FONTSIZE)
    ax.yaxis.label.set_fontsize(AXIS_LABEL_FONTSIZE)
    ax.tick_params(axis="both", labelsize=TICK_FONTSIZE)


def setup_mpl(font_size: int = 17, agg: bool = True) -> None:
    """Configure matplotlib for non-interactive PNG output."""
    if agg:
        matplotlib.use("Agg")
    matplotlib.rcParams.update({"font.size": font_size})


def add_reference_lines(
    ax: Axes,
    *,
    median: float,
    median_label: str = "median",
    median_fmt: str = ".1f",
    thresholds: Sequence[float] = (10, 20),
    threshold_colors: Sequence[str] = THRESHOLD_COLORS,
    zero_label: str = "0%",
) -> None:
    """Draw median (red dashed), 0 (solid black), and symmetric threshold lines.

    Thresholds are in the same units as the x-axis (e.g. percent points).
    Pairs each threshold with a color from ``threshold_colors``; symmetric
    ±threshold lines share a legend entry.
    """
    ax.axvline(
        x=median,
        color=MEDIAN_COLOR,
        linestyle="--",
        linewidth=1.5,
        label=f"{median_label} = {median:{median_fmt}}%",
    )
    ax.axvline(x=0, color=ZERO_COLOR, linestyle="-", linewidth=2, label=zero_label)
    for t, color in zip(thresholds, threshold_colors):
        ax.axvline(
            x=-t,
            color=color,
            linestyle="--",
            linewidth=1.5,
            alpha=0.8,
            label=f"±{t:g}%",
        )
        ax.axvline(x=t, color=color, linestyle="--", linewidth=1.5, alpha=0.8)


def format_percent_axis(ax: Axes, ticks: Sequence[float] = PERCENT_TICKS) -> None:
    """Apply fixed tick locations and percent formatting to the x-axis."""
    ax.xaxis.set_major_locator(mticker.FixedLocator(list(ticks)))
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=100, decimals=0))


def add_text_box(
    ax: Axes,
    text: str,
    *,
    loc: str = "upper left",
    fontsize: int = 13,
    multialignment: str | None = None,
    bg_alpha: float = 0.35,
) -> None:
    """Anchor a wheat-backed monospace text box to a corner of the axes.

    ``multialignment`` controls how lines line up within the box (independent
    of the box anchor); defaults to match the corner's horizontal alignment.
    ``bg_alpha`` controls the background opacity — lower values let the
    underlying plot show through.
    """
    positions = {
        "upper left": (0.02, 0.95, "top", "left"),
        "upper right": (0.98, 0.95, "top", "right"),
        "lower left": (0.02, 0.05, "bottom", "left"),
        "lower right": (0.98, 0.05, "bottom", "right"),
    }
    x, y, va, ha = positions[loc]
    ax.text(
        x,
        y,
        text,
        transform=ax.transAxes,
        fontsize=fontsize,
        verticalalignment=va,
        horizontalalignment=ha,
        multialignment=multialignment if multialignment is not None else ha,
        fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.3", facecolor="wheat", alpha=bg_alpha),
    )


def percent_histogram(
    ax: Axes,
    vals_pct: "pd.Series[float]",
    *,
    xlim: tuple[float, float] = DEFAULT_XLIM,
    bin_width: float = DEFAULT_BIN_WIDTH,
    ticks: Sequence[float] = PERCENT_TICKS,
    xlabel: str = "Percentage Diff (%)",
    ylabel: str = "Count",
    title: str | None = None,
    text_box: str | None = None,
    text_box_loc: str = "upper left",
    text_box_fontsize: int = 13,
    legend_loc: str = "upper right",
    legend_fontsize: int = 14,
    thresholds: Sequence[float] = (10, 20),
) -> None:
    """Render a percent-diff histogram panel with reference lines + text box."""
    vals = vals_pct.dropna()
    bin_edges = np.arange(xlim[0], xlim[1] + bin_width, bin_width).tolist()
    ax.hist(vals, bins=bin_edges, alpha=HIST_ALPHA, color=HIST_COLOR)

    add_reference_lines(ax, median=float(vals.median()), thresholds=thresholds)
    ax.set_xlim(xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title is not None:
        ax.set_title(title)
    format_percent_axis(ax, ticks)
    ax.legend(fontsize=legend_fontsize, loc=legend_loc)

    if text_box:
        add_text_box(
            ax,
            text_box,
            loc=text_box_loc,
            fontsize=text_box_fontsize,
            multialignment="left",
        )


def normalize_pct_diff_to_percent(raw: "pd.Series[Any]") -> "pd.Series[float]":
    """Normalize a diagnostics ``*_perc_diff`` column to percent units.

    Diagnostics Sheets store percent diffs in two conventions:
    - percent-formatted strings, e.g. ``"-6.86%"`` (already in percent)
    - bare fractions, e.g. ``"0.0155"`` (= 1.55%, needs ×100)

    Cells containing ``%`` are parsed as percent; bare numerics are treated as
    fractions and scaled to percent. Non-parseable cells become NaN. This lets
    one overlay mix older (``%``-string) and newer (fraction) runs safely.
    """
    s = raw.astype(str).str.strip()
    has_pct = s.str.contains("%", na=False)
    num = pd.to_numeric(s.str.replace("%", "", regex=False), errors="coerce")
    return num.where(has_pct, num * 100.0)


def overlay_pct_diff_histogram(
    ax: Axes,
    series_by_label: dict[str, "pd.Series[float]"],
    *,
    colors: dict[str, str] | None = None,
    xlim: tuple[float, float] = (-100.0, 100.0),
    bin_width: float = 5.0,
    beyond_threshold: float = 20.0,
    xlabel: str = "EF change vs baseline (%)",
    ylabel: str = "sector count",
    title: str | None = None,
    legend_loc: str = "upper right",
    legend_fontsize: int = LEGEND_FONTSIZE,
) -> None:
    """Overlay multiple percent-diff distributions on one axis.

    Each series renders as a semi-transparent histogram with a dashed median
    line; the legend reports per-series ``median``, ``% up`` (share > 0), and
    ``% beyond ±threshold``. Inputs are in percent units (run values through
    :func:`normalize_pct_diff_to_percent` first). Bars are clipped to ``xlim``;
    stats are computed on the unclipped values so tails are not hidden.
    """
    palette = ("#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b")
    bin_edges = np.arange(xlim[0], xlim[1] + bin_width, bin_width).tolist()
    for i, (label, raw) in enumerate(series_by_label.items()):
        vals = pd.to_numeric(raw, errors="coerce").dropna()
        if vals.empty:
            continue
        color = (colors or {}).get(label) or palette[i % len(palette)]
        median = float(vals.median())
        pct_up = float((vals > 0).mean() * 100.0)
        beyond = float((vals.abs() > beyond_threshold).mean() * 100.0)
        legend = (
            f"{label} (n={len(vals)})\nmedian {median:+.0f}% | {pct_up:.0f}% up | "
            f"{beyond:.0f}% beyond ±{beyond_threshold:g}%"
        )
        ax.hist(
            vals.clip(xlim[0], xlim[1]),
            bins=bin_edges,
            color=color,
            alpha=0.5,
            label=legend,
        )
        ax.axvline(median, color=color, linestyle="--", linewidth=1.3)
    ax.axvline(0, color=ZERO_COLOR, linewidth=1.0)
    ax.set_xlim(xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title is not None:
        ax.set_title(title)
    format_percent_axis(ax, PERCENT_TICKS)
    ax.grid(True, ls=":", alpha=0.3)
    ax.legend(fontsize=legend_fontsize, loc=legend_loc, framealpha=0.5)


def abs_change_histogram(
    ax: Axes,
    values: "pd.Series[float]",
    *,
    clip_range: tuple[float, float],
    n_bins: int = 100,
    outlier_label_fn: Callable[[Hashable, float], str] | None = None,
    value_fmt: str = "+.4f",
    xlabel: str = "Absolute change",
    ylabel: str = "Count",
    title: str | None = None,
    increase_label: str = "Increases",
    decrease_label: str = "Decreases",
    legend_loc: str = "upper center",
    legend_fontsize: int = 14,
    outlier_fontsize: int = 11,
    outlier_bg_alpha: float = 0.35,
    outlier_text_align: str = "left",
) -> None:
    """Histogram of absolute changes, clipped to ``clip_range``.

    Outliers outside ``clip_range`` are listed in two wheat boxes at
    top-left (decreases) and top-right (increases). Callers pass
    ``outlier_label_fn(index, value) -> str`` to customize each row; by
    default shows just the index and value. ``outlier_bg_alpha`` lowers
    the box opacity so bars stay visible; ``outlier_text_align`` sets
    line-alignment inside the boxes.
    """
    vals = values.dropna()
    clipped_below = int((vals < clip_range[0]).sum())
    clipped_above = int((vals > clip_range[1]).sum())
    visible = vals.clip(lower=clip_range[0], upper=clip_range[1])

    bin_edges = np.linspace(clip_range[0], clip_range[1], n_bins + 1).tolist()
    ax.hist(visible, bins=bin_edges, alpha=HIST_ALPHA, color=HIST_COLOR)

    median_val = float(vals.median())
    std_val = float(vals.std())
    ax.axvline(
        median_val,
        color=MEDIAN_COLOR,
        linestyle="--",
        linewidth=1.5,
        label=f"median = {median_val:{value_fmt}}  (std: {std_val:{value_fmt.lstrip('+')}})",
    )
    ax.axvline(x=0, color=ZERO_COLOR, linestyle="-", linewidth=2, label="0")

    ax.set_xlim(clip_range)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if title is not None:
        ax.set_title(title)
    ax.legend(fontsize=legend_fontsize, loc=legend_loc)

    above = vals[vals > clip_range[1]].sort_values(ascending=False)
    below = vals[vals < clip_range[0]].sort_values()

    def _format(idx: Hashable, v: float) -> str:
        if outlier_label_fn is not None:
            return outlier_label_fn(idx, v)
        return f"  {idx}: {v:{value_fmt}}"

    dec_lines = [
        f"Clipped: {clipped_below} below {clip_range[0]:g}",
        "",
        f"{decrease_label} below {clip_range[0]:g}:",
        *[_format(idx, v) for idx, v in below.items()],
    ]
    inc_lines = [
        f"Clipped: {clipped_above} above {clip_range[1]:g}",
        "",
        f"{increase_label} above {clip_range[1]:g}:",
        *[_format(idx, v) for idx, v in above.items()],
    ]
    add_text_box(
        ax,
        "\n".join(dec_lines),
        loc="upper left",
        fontsize=outlier_fontsize,
        multialignment=outlier_text_align,
        bg_alpha=outlier_bg_alpha,
    )
    add_text_box(
        ax,
        "\n".join(inc_lines),
        loc="upper right",
        fontsize=outlier_fontsize,
        multialignment=outlier_text_align,
        bg_alpha=outlier_bg_alpha,
    )


def dodge_annotations(
    ax: Axes,
    items: Sequence[tuple[float, float, str]],
    *,
    fontsize: int = 12,
    offset_px: tuple[float, float] = (6, 6),
    max_iters: int = 60,
    step_px: float = 3.0,
    max_push_px: float = 40.0,
    leader_threshold_px: float = 16.0,
    avoid: Sequence[Any] | None = None,
) -> None:
    """Place text annotations at each ``(x, y)`` and nudge overlaps apart vertically.

    Greedy: give every label the initial offset, then for each overlapping pair
    push the upper one up. If that label has already been pushed past
    ``max_push_px`` above its starting offset, push the lower one down instead
    (capped symmetrically). Stops when no pair overlaps or ``max_iters`` is
    hit. A thin leader line is kept on labels that drift noticeably from their
    point so the reader can match label to data. ``avoid`` is a sequence of
    artists (e.g. existing text boxes or legends) whose bboxes are treated as
    immovable obstacles — labels that collide with them are pushed downward.
    """
    if not items:
        return
    fig = ax.figure
    if fig is None:
        return

    arrow_style = dict(arrowstyle="-", color="0.6", lw=0.5, shrinkA=0, shrinkB=2)
    annotations = [
        ax.annotate(
            text,
            xy=(x, y),
            xytext=offset_px,
            textcoords="offset points",
            fontsize=fontsize,
            ha="left",
            va="bottom",
            arrowprops=arrow_style,
        )
        for x, y, text in items
    ]
    base_dy = offset_px[1]
    fig.canvas.draw()
    renderer = cast(_FigureCanvasWithRenderer, fig.canvas).get_renderer()
    axes_bbox = ax.get_window_extent(renderer=renderer)
    avoid_bboxes = (
        [a.get_window_extent(renderer=renderer) for a in avoid] if avoid else []
    )

    for _ in range(max_iters):
        bboxes = [t.get_window_extent(renderer=renderer) for t in annotations]
        moved = False
        # Push any annotation colliding with an avoid-region downward.
        for k in range(len(annotations)):
            if any(bboxes[k].overlaps(ab) for ab in avoid_bboxes):
                dx, dy = annotations[k].xyann
                if base_dy - dy < max_push_px:
                    annotations[k].xyann = (dx, dy - step_px)
                    moved = True
        for i in range(len(annotations)):
            for j in range(i + 1, len(annotations)):
                if not bboxes[i].overlaps(bboxes[j]):
                    continue
                upper = i if bboxes[i].y0 >= bboxes[j].y0 else j
                lower = j if upper == i else i
                dx_u, dy_u = annotations[upper].xyann
                dx_l, dy_l = annotations[lower].xyann
                upper_top = bboxes[upper].y1
                upper_collides_avoid = any(
                    bboxes[upper].overlaps(ab) for ab in avoid_bboxes
                )
                can_push_up = (
                    dy_u - base_dy < max_push_px
                    and upper_top + step_px < axes_bbox.y1
                    and not upper_collides_avoid
                )
                if can_push_up:
                    annotations[upper].xyann = (dx_u, dy_u + step_px)
                    moved = True
                elif base_dy - dy_l < max_push_px:
                    annotations[lower].xyann = (dx_l, dy_l - step_px)
                    moved = True
        if not moved:
            break
        fig.canvas.draw()

    for t in annotations:
        dx, dy = t.xyann
        if (dx**2 + dy**2) ** 0.5 <= leader_threshold_px and t.arrow_patch is not None:
            t.arrow_patch.set_visible(False)


def _order_stack_for_net_bar(df: pd.DataFrame, *, positive: bool) -> pd.DataFrame:
    """Sort segments for stacked net bar: large magnitude near zero; Other at far end."""
    ordered = df.copy()
    sector_normalized = ordered["sector"].str.strip().str.lower()

    if positive:
        ordered["_is_other_end"] = sector_normalized.str.startswith("other increase")
        ordered = ordered.sort_values(
            by=["_is_other_end", "value"],
            ascending=[True, False],
        )
    else:
        ordered["_is_other_end"] = sector_normalized.str.startswith("other decrease")
        ordered = ordered.sort_values(
            by=["_is_other_end", "value"],
            ascending=[True, True],
        )

    return ordered.drop(columns="_is_other_end")


_STACKED_BAR_WIDTH = 0.8
_STACKED_NET_MARKER_SIZE = 80
_STACKED_CALLOUT_LINE_COLOR = "0.6"
_STACKED_CALLOUT_LINE_WIDTH = 0.7
_STACKED_CALLOUT_X_OFFSETS: tuple[float, ...] = (0.8, 1.0, 1.2)


def plot_stacked_net_change(
    ax: Axes,
    df: pd.DataFrame,
    *,
    title: str,
    ylabel: str,
) -> None:
    """Single stacked bar from zero: positives up, negatives down, net scatter + label.

    ``df`` must have string ``sector`` and numeric ``value`` columns. Caller
    owns the figure — matches the ``(ax, data, ...)`` signature of
    ``percent_histogram`` and ``abs_change_histogram``.
    """
    pos = _order_stack_for_net_bar(df.loc[df["value"] > 0], positive=True)
    neg = _order_stack_for_net_bar(df.loc[df["value"] < 0], positive=False)
    label_side_index = 0

    pos_bottom = 0.0
    neg_bottom = 0.0
    total_span = max(pos["value"].sum() - neg["value"].sum(), 1.0)
    inside_label_threshold = total_span * 0.08
    callout_arrow = {
        "arrowstyle": "-",
        "lw": _STACKED_CALLOUT_LINE_WIDTH,
        "color": _STACKED_CALLOUT_LINE_COLOR,
        "shrinkA": 0,
        "shrinkB": 0,
    }

    def add_segment_label(*, sector: str, value: float, bottom: float) -> None:
        nonlocal label_side_index
        center_y = bottom + value / 2
        sector_normalized = sector.strip().lower()

        if sector_normalized.startswith(("other increase", "other decrease")):
            va = "top" if value > 0 else "bottom"
            y_text = (
                bottom + value - 0.02 * total_span
                if value > 0
                else bottom + value + 0.02 * total_span
            )
            ax.text(0, y_text, sector, ha="center", va=va, fontsize=TEXT_BOX_FONTSIZE)
            return

        if abs(value) >= inside_label_threshold:
            ax.text(
                0,
                center_y,
                sector,
                ha="center",
                va="center",
                fontsize=TEXT_BOX_FONTSIZE,
            )
            return

        is_right_side = label_side_index % 2 == 0
        x_offset = _STACKED_CALLOUT_X_OFFSETS[
            (label_side_index // 2) % len(_STACKED_CALLOUT_X_OFFSETS)
        ]
        x_text = x_offset if is_right_side else -x_offset
        ha = "left" if is_right_side else "right"
        label_side_index += 1
        ax.annotate(
            sector,
            xy=(0, center_y),
            xytext=(x_text, center_y),
            ha=ha,
            va="center",
            fontsize=TEXT_BOX_FONTSIZE,
            arrowprops=callout_arrow,
        )

    for _, row in pos.iterrows():
        value = float(row["value"])
        sector = str(row["sector"])
        ax.bar(0, value, bottom=pos_bottom, width=_STACKED_BAR_WIDTH)
        add_segment_label(sector=sector, value=value, bottom=pos_bottom)
        pos_bottom += value

    for _, row in neg.iterrows():
        value = float(row["value"])
        sector = str(row["sector"])
        ax.bar(0, value, bottom=neg_bottom, width=_STACKED_BAR_WIDTH)
        add_segment_label(sector=sector, value=value, bottom=neg_bottom)
        neg_bottom += value

    net_total = float(df["value"].sum())
    ax.scatter(0, net_total, s=_STACKED_NET_MARKER_SIZE, color="black", zorder=5)
    ax.annotate(
        "Net total",
        xy=(0, net_total),
        xycoords="data",
        xytext=(0.98, 0.98),
        textcoords="axes fraction",
        ha="right",
        va="top",
        fontsize=TEXT_BOX_FONTSIZE,
        fontweight="bold",
        arrowprops=callout_arrow,
    )

    ax.axhline(0, color="black", linewidth=1)
    ax.set_xticks([0])
    ax.set_xticklabels([f"Net change = {net_total:,.0f} MMT CO2e"])
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=TITLE_FONTSIZE, pad=12)
    y_formatter = mticker.ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    ax.yaxis.set_major_formatter(y_formatter)
    max_callout_x = max(_STACKED_CALLOUT_X_OFFSETS, default=1.2)
    ax.set_xlim(-(max_callout_x + 0.4), max_callout_x + 0.4)
    apply_axis_fonts(ax)


def save_and_close(fig: Figure, out: Path) -> None:
    """Write ``fig`` to ``out`` at dpi=150, close the figure, and log the path."""
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")
