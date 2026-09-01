"""Waterfall and D/A-effect figures for CEDA electricity decks."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes

from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.data import (
    Scope,
    effect_frame,
    filter_scope,
    total_bly,
)
from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.sheets import (
    LadderStep,
)
from bedrock.utils.validation.analysis.plotting import save_and_close, setup_mpl

BLUE = '#3a6ea5'
GREEN = '#2e7d32'
RED = '#c62828'


def write_da_effect_scatter(
    n_df,
    path: Path,
    *,
    scope: Scope,
) -> Path:
    frame = effect_frame(filter_scope(n_df, scope))
    setup_mpl(font_size=11)
    fig, ax = plt.subplots(figsize=(9.0, 7.0))
    if frame.empty:
        ax.text(0.5, 0.5, 'no data', ha='center', va='center', transform=ax.transAxes)
    else:
        ax.scatter(
            frame['d_effect'],
            frame['a_effect'],
            s=10,
            alpha=0.35,
            c=BLUE,
            edgecolors='none',
        )
        lim = max(
            20.0,
            float(np.nanpercentile(frame['d_effect'].abs(), 99)),
            float(np.nanpercentile(frame['a_effect'].abs(), 99)),
        )
        ax.axhline(0, color='black', lw=1)
        ax.axvline(0, color='black', lw=1)
        ax.set_xlim(-lim, lim)
        ax.set_ylim(-lim, lim)
        ax.set_xlabel('N D-effect (% points)')
        ax.set_ylabel('N A-effect (% points)')
        med_d = float(frame['d_effect'].median())
        med_a = float(frame['a_effect'].median())
        ax.set_title(
            f'{scope}: N change decomposition '
            f'(median D-effect={med_d:+.2f}pp, A-effect={med_a:+.2f}pp)'
        )
    fig.tight_layout()
    save_and_close(fig, path)
    return path


def _draw_net_waterfall(
    ax: Axes,
    levels: list[float],
    labels: list[str],
    *,
    title: str,
    ylabel: str,
) -> None:
    """Net waterfall: start level, signed step deltas, endpoint total."""
    if len(levels) < 2:
        ax.text(0.5, 0.5, 'no data', ha='center', va='center', transform=ax.transAxes)
        ax.set_title(title)
        return

    deltas = [levels[i] - levels[i - 1] for i in range(1, len(levels))]
    n_bars = 1 + len(deltas) + 1
    xs = list(range(n_bars))
    bottoms: list[float] = []
    heights: list[float] = []
    colors: list[str] = []
    display: list[float] = []

    bottoms.append(0.0)
    heights.append(levels[0])
    colors.append(BLUE)
    display.append(levels[0])

    running = levels[0]
    for d in deltas:
        if d >= 0:
            bottoms.append(running)
            heights.append(d)
            colors.append(GREEN)
        else:
            bottoms.append(running + d)
            heights.append(-d)
            colors.append(RED)
        display.append(d)
        running += d

    bottoms.append(0.0)
    heights.append(levels[-1])
    colors.append(BLUE)
    display.append(levels[-1])

    tick_labels = [labels[0], *[f'Δ{lab}' for lab in labels[1:]], labels[-1]]
    ax.bar(xs, heights, bottom=bottoms, color=colors, edgecolor='black', linewidth=0.5)
    pad = max(abs(x) for x in levels) * 0.015 + 1e-9
    for x, h, b, val in zip(xs, heights, bottoms, display, strict=True):
        ax.text(x, b + h + pad, f'{val:,.1f}', ha='center', va='bottom', fontsize=8)
    ax.set_xticks(xs)
    ax.set_xticklabels(tick_labels, fontsize=9)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.axhline(0, color='black', lw=0.8)


def write_bly_ladder_waterfall(
    steps: tuple[LadderStep, ...],
    bly_frames: dict,
    path: Path,
    *,
    scope: Scope,
) -> Path:
    levels = [total_bly(bly_frames[s.key], scope) for s in steps]
    labels = [s.key for s in steps]
    setup_mpl(font_size=11)
    fig, ax = plt.subplots(figsize=(12.0, 6.2))
    _draw_net_waterfall(
        ax,
        levels,
        labels,
        title=f'{scope} attributed BLy (MtCO2e) — cumulative g1→g5e',
        ylabel='BLy (MtCO2e)',
    )
    fig.tight_layout()
    save_and_close(fig, path)
    return path
