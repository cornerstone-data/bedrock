"""BLy dispersion waterfall renderer (greenfield; CEDA dispersion.py visual reference)."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from bedrock.analysis.electricity_disagg_diagnostics.dispersion import ChainedDispersion
from bedrock.analysis.electricity_disagg_diagnostics.net_change import (
    ChainedNetChange,
    NetChangeBar,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR, ensure_dirs
from bedrock.utils.validation.analysis.plotting import save_and_close

BLUE = '#3a6ea5'
GREY = '#9e9e9e'
DARKGREY = '#616161'

TITLE = (
    'U.S. attributed emissions: electricity disaggregation drivers\n'
    'chained PR2→PR4 vs Cornerstone v0.2 footing'
)
FOOTNOTE_CHAINED = (
    'Each step bar = gross cross-sector reallocation (Σ|ΔBLy|) for one chained '
    'PR2→PR4 transition. Sector oscillation across steps can make the sum of step '
    'bars exceed the combined FINAL bar; the offsetting bar reconciles that gap.'
)
NET_TITLE = (
    'U.S. total attributed BLy: electricity disaggregation net change\n'
    'chained PR2→PR4 vs Cornerstone v0.2 footing'
)
FOOTNOTE_NET = (
    'Level bars = Σ BLy_new (MMT CO2e) at each model step. A delta bar appears '
    'only when total U.S. BLy changes between consecutive steps (after 221100↔children '
    'alignment). This is net national change, not gross cross-sector reallocation.'
)

GREEN = '#2e7d32'
RED = '#c62828'


def _format_value(value: float, *, use_pct: bool) -> str:
    if use_pct:
        return f'{value:.1f}%'
    return f'{value:,.2f}'


def _draw_step_bars(
    ax: Axes,
    step_values: list[float],
    *,
    use_pct: bool,
    label_color: str,
) -> float:
    cum = 0.0
    top = sum(step_values) or 1.0
    for i, dv in enumerate(step_values):
        ax.bar(
            i,
            dv,
            bottom=cum,
            width=0.64,
            color=BLUE,
            edgecolor='black',
            linewidth=0.6,
            zorder=3,
        )
        ax.text(
            i,
            cum + dv + top * 0.01,
            _format_value(dv, use_pct=use_pct),
            ha='center',
            va='bottom',
            fontsize=10,
            color=label_color,
            fontweight='bold',
        )
        cum += dv
    return cum


def render_dispersion_waterfall(
    result: ChainedDispersion,
    *,
    use_pct: bool,
    out_path: Path,
) -> None:
    ensure_dirs()
    step_values = result.step_values_pct if use_pct else result.step_values_mmt
    combined = result.combined_pct if use_pct else result.combined_mmt
    offset = result.offset_pct if use_pct else result.offset_mmt
    sum_steps = sum(step_values)

    if result.show_offsetting_bar:
        tail_labels = ['Offsetting\n& overlap', 'Combined\n(FINAL)']
    else:
        tail_labels = ['Combined\n(FINAL)']
    labels = result.step_labels + tail_labels
    n = len(labels)
    fig, ax = plt.subplots(figsize=(max(11, 1.3 * n), 7))

    _draw_step_bars(ax, step_values, use_pct=use_pct, label_color=BLUE)

    cum = sum_steps
    next_i = len(step_values)
    if result.show_offsetting_bar:
        ax.bar(
            next_i,
            offset,
            bottom=combined,
            width=0.64,
            color=GREY,
            edgecolor='black',
            linewidth=0.6,
            zorder=3,
        )
        ax.text(
            next_i,
            cum + (sum_steps or 1.0) * 0.01,
            f'-{_format_value(offset, use_pct=use_pct)}',
            ha='center',
            va='bottom',
            fontsize=10,
            color=DARKGREY,
            fontweight='bold',
        )
        next_i += 1

    ax.bar(
        next_i,
        combined,
        width=0.64,
        color=DARKGREY,
        edgecolor='black',
        linewidth=0.6,
        zorder=3,
    )
    ax.text(
        next_i,
        combined + (sum_steps or 1.0) * 0.01,
        _format_value(combined, use_pct=use_pct),
        ha='center',
        va='bottom',
        fontsize=11,
        color='black',
        fontweight='bold',
    )

    running = 0.0
    for i, dv in enumerate(step_values):
        running += dv
        ax.plot(
            [i + 0.32, i + 1 - 0.32],
            [running, running],
            ls='--',
            lw=1.0,
            color='0.6',
            zorder=2,
        )

    y_top = max(sum_steps, combined) * 1.12 or 1.0
    ax.set_xticks(range(n))
    ax.set_xticklabels(labels, fontsize=9.5)
    ax.set_ylim(0, y_top)
    if use_pct:
        ylabel = (
            'Change in attributed emissions Σ|ΔBLy| '
            '(% of Cornerstone v0.2 U.S. total BLy)'
        )
    else:
        ylabel = 'Cross-sector reallocation Σ|ΔBLy| (MMT CO2e)\n[sum over USA sectors]'
    ax.set_ylabel(ylabel)
    ax.set_title(TITLE, fontsize=13, pad=12)
    ax.grid(axis='y', ls=':', alpha=0.4)
    ax.annotate(
        FOOTNOTE_CHAINED,
        xy=(0.5, -0.15),
        xycoords='axes fraction',
        ha='center',
        va='top',
        fontsize=8.3,
        color='0.35',
    )
    fig.tight_layout()
    save_and_close(fig, out_path)


def write_waterfall_pngs(result: ChainedDispersion) -> tuple[Path, Path]:
    ensure_dirs()
    mmt_path = OUT_DIR / 'electricity_bly_dispersion_waterfall_mmt.png'
    pct_path = OUT_DIR / 'electricity_bly_dispersion_waterfall_pct.png'
    render_dispersion_waterfall(result, use_pct=False, out_path=mmt_path)
    render_dispersion_waterfall(result, use_pct=True, out_path=pct_path)
    return mmt_path, pct_path


def _delta_bar_color(delta_mmt: float) -> str:
    if delta_mmt >= 0:
        return GREEN
    return RED


def _annotate_bar(
    ax: Axes,
    idx: int,
    bar: NetChangeBar,
    *,
    use_pct: bool,
    result: ChainedNetChange,
    y_pad: float,
) -> None:
    if bar.kind == 'level':
        y = result.bar_value(bar, use_pct=use_pct) + y_pad
        text = _format_value(result.bar_value(bar, use_pct=use_pct), use_pct=use_pct)
        color = DARKGREY if idx == 0 else BLUE
    else:
        assert bar.signed_delta is not None
        top = result.bar_bottom(bar, use_pct=use_pct) + result.bar_value(
            bar, use_pct=use_pct
        )
        y = top + y_pad
        prefix = '+' if bar.signed_delta >= 0 else '-'
        text = prefix + _format_value(
            result.bar_value(bar, use_pct=use_pct), use_pct=use_pct
        )
        color = _delta_bar_color(bar.signed_delta)
    ax.text(
        idx,
        y,
        text,
        ha='center',
        va='bottom',
        fontsize=9 if bar.kind == 'delta' else 10,
        color=color,
        fontweight='bold',
    )


def render_net_change_waterfall(
    result: ChainedNetChange,
    *,
    use_pct: bool,
    out_path: Path,
) -> None:
    ensure_dirs()
    bars = result.build_bars()
    n = len(bars)
    fig, ax = plt.subplots(figsize=(max(11, 1.15 * n), 7))

    level_values = [
        result.bar_value(b, use_pct=use_pct) for b in bars if b.kind == 'level'
    ]
    delta_values = [
        result.bar_value(b, use_pct=use_pct) for b in bars if b.kind == 'delta'
    ]
    y_top = (max(level_values) if level_values else 1.0) * 1.06
    if delta_values:
        y_top = max(
            y_top,
            max(
                result.bar_bottom(b, use_pct=use_pct)
                + result.bar_value(b, use_pct=use_pct)
                for b in bars
                if b.kind == 'delta'
            )
            * 1.06,
        )
    y_pad = y_top * 0.008

    for idx, bar in enumerate(bars):
        height = result.bar_value(bar, use_pct=use_pct)
        bottom = result.bar_bottom(bar, use_pct=use_pct)
        if bar.kind == 'level':
            color = DARKGREY if idx == 0 else BLUE
            ax.bar(
                idx,
                height,
                width=0.64,
                color=color,
                edgecolor='black',
                linewidth=0.6,
                zorder=3,
            )
        else:
            assert bar.signed_delta is not None
            ax.bar(
                idx,
                height,
                bottom=bottom,
                width=0.64,
                color=_delta_bar_color(bar.signed_delta),
                edgecolor='black',
                linewidth=0.6,
                zorder=4,
            )
        _annotate_bar(ax, idx, bar, use_pct=use_pct, result=result, y_pad=y_pad)

    ax.set_xticks(range(n))
    ax.set_xticklabels([b.label for b in bars], fontsize=8.5)
    ax.set_ylim(0, y_top)
    if use_pct:
        ylabel = (
            'Total U.S. attributed BLy (% of Cornerstone v0.2 footing total)\n'
            '[level bars]; net step change [%]'
        )
    else:
        ylabel = (
            'Total U.S. attributed BLy (MMT CO2e)\n[level bars]; net step change [MMT]'
        )
    ax.set_ylabel(ylabel)
    ax.set_title(NET_TITLE, fontsize=13, pad=12)
    ax.grid(axis='y', ls=':', alpha=0.4)
    ax.annotate(
        FOOTNOTE_NET,
        xy=(0.5, -0.18),
        xycoords='axes fraction',
        ha='center',
        va='top',
        fontsize=8.3,
        color='0.35',
    )
    fig.tight_layout()
    save_and_close(fig, out_path)


def write_net_change_waterfall_pngs(result: ChainedNetChange) -> tuple[Path, Path]:
    ensure_dirs()
    mmt_path = OUT_DIR / 'electricity_bly_net_change_waterfall_mmt.png'
    pct_path = OUT_DIR / 'electricity_bly_net_change_waterfall_pct.png'
    render_net_change_waterfall(result, use_pct=False, out_path=mmt_path)
    render_net_change_waterfall(result, use_pct=True, out_path=pct_path)
    return mmt_path, pct_path
