"""Histogram PNG helpers for CEDA electricity decks."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.data import (
    Scope,
    d_perc_no_ma,
    filter_scope,
    n_perc_no_ma,
    n_perc_with_ma,
)
from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.sheets import (
    LadderStep,
)
from bedrock.utils.validation.analysis.plotting import (
    DEFAULT_XLIM,
    apply_axis_fonts,
    percent_histogram,
    save_and_close,
    setup_mpl,
)


def _stats_box(vals_pct: pd.Series) -> str:
    v = vals_pct.dropna().to_numpy(dtype=float)
    if v.size == 0:
        return 'n=0'
    beyond = int(np.sum(np.abs(v) > 20.0))
    return (
        f'n={v.size}\n'
        f'median={np.median(v):+.2f}%\n'
        f'p95(|·|)={np.percentile(np.abs(v), 95):.1f}%\n'
        f'|x|>20%: {beyond}'
    )


def write_nd_hist_pair(
    n_df: pd.DataFrame,
    d_df: pd.DataFrame,
    path: Path,
    *,
    scope: Scope,
    title_prefix: str,
) -> Path:
    n_vals = n_perc_no_ma(filter_scope(n_df, scope))
    d_vals = d_perc_no_ma(filter_scope(d_df, scope))
    setup_mpl(font_size=11)
    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.8))
    for ax, vals, kind in (
        (axes[0], n_vals, 'N'),
        (axes[1], d_vals, 'D'),
    ):
        label = 'total EF (N)' if kind == 'N' else 'direct EF (D)'
        if vals.empty:
            ax.text(
                0.5, 0.5, 'no data', ha='center', va='center', transform=ax.transAxes
            )
            ax.set_title(f'{title_prefix}: {label}')
            continue
        percent_histogram(
            ax,
            vals,
            xlim=DEFAULT_XLIM,
            title=f'{title_prefix}: {label}',
            text_box=_stats_box(vals),
            text_box_fontsize=8,
            legend_fontsize=9,
        )
        apply_axis_fonts(ax)
    fig.suptitle(
        f'{scope} per-sector % diff vs CEDA v8.1 (no manual adj)',
        fontsize=14,
    )
    fig.tight_layout(rect=(0, 0.02, 1, 0.94))
    save_and_close(fig, path)
    return path


def write_manual_adj_compare(
    n_df: pd.DataFrame,
    path: Path,
    *,
    scope: Scope,
) -> Path:
    scoped = filter_scope(n_df, scope)
    no_ma = n_perc_no_ma(scoped)
    with_ma = n_perc_with_ma(scoped)
    setup_mpl(font_size=11)
    fig, axes = plt.subplots(1, 2, figsize=(14.0, 5.8))
    for ax, vals, label in (
        (axes[0], no_ma, 'vs v8.1 no manual adj'),
        (axes[1], with_ma, 'vs v8.1 with manual adj'),
    ):
        if vals.empty:
            ax.text(
                0.5, 0.5, 'no data', ha='center', va='center', transform=ax.transAxes
            )
            ax.set_title(label)
            continue
        percent_histogram(
            ax,
            vals,
            xlim=DEFAULT_XLIM,
            title=label,
            text_box=_stats_box(vals),
            text_box_fontsize=8,
            legend_fontsize=9,
        )
        apply_axis_fonts(ax)
    fig.suptitle(f'{scope} N % diff — baseline choice', fontsize=14)
    fig.tight_layout(rect=(0, 0.02, 1, 0.94))
    save_and_close(fig, path)
    return path


def write_ladder_hist_panels(
    steps: tuple[LadderStep, ...],
    frames: dict[str, pd.DataFrame],
    path: Path,
    *,
    kind: str,
    scope: Scope,
) -> Path:
    setup_mpl(font_size=10)
    n = len(steps)
    fig, axes = plt.subplots(1, n, figsize=(4.2 * n, 5.2), squeeze=False)
    y_max = 0.0
    for i, step in enumerate(steps):
        ax = axes[0][i]
        df = filter_scope(frames[step.key], scope)
        vals = n_perc_no_ma(df) if kind == 'N' else d_perc_no_ma(df)
        if vals.empty:
            ax.text(
                0.5, 0.5, 'no data', ha='center', va='center', transform=ax.transAxes
            )
            ax.set_title(step.title, fontsize=10)
            continue
        percent_histogram(
            ax,
            vals,
            xlim=DEFAULT_XLIM,
            title=step.title,
            text_box=_stats_box(vals),
            text_box_fontsize=7,
            legend_fontsize=7,
        )
        apply_axis_fonts(ax)
        y_max = max(y_max, ax.get_ylim()[1])
    if y_max > 0:
        for i in range(n):
            axes[0][i].set_ylim(0, y_max)
    kind_label = 'total EF (N)' if kind == 'N' else 'direct EF (D)'
    fig.suptitle(
        f'{scope} {kind_label} % diff vs CEDA v8.1 (no manual adj) — g1→g5e',
        fontsize=13,
    )
    fig.tight_layout(rect=(0, 0.02, 1, 0.92))
    save_and_close(fig, path)
    return path
