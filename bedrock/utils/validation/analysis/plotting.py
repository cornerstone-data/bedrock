"""Shared plotting primitives for the diagnostics analysis package.

Centralizes styling (colors, reference lines, text boxes, axis formatters)
and the label-dodge helper so the plot scripts in this package share a
consistent look. Stays close to matplotlib: callers build the figure/axes,
pass in data, and get styled panels back.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable, Sequence
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

DEFAULT_XLIM: tuple[float, float] = (-115.0, 115.0)
PERCENT_TICKS: tuple[float, ...] = (-100, -50, -20, 0, 20, 50, 100)
DEFAULT_BIN_WIDTH: float = 2.0

HIST_COLOR = "steelblue"
HIST_ALPHA = 0.6
MEDIAN_COLOR = "red"
ZERO_COLOR = "black"
THRESHOLD_COLORS: tuple[str, str] = ("green", "purple")


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
    renderer = fig.canvas.get_renderer()
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


def save_and_close(fig: Figure, out: Path) -> None:
    """Write ``fig`` to ``out`` at dpi=150, close the figure, and log the path."""
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out}")
