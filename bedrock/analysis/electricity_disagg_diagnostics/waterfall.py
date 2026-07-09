"""BLy dispersion waterfall renderer (greenfield; CEDA dispersion.py visual reference)."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.axes import Axes

from bedrock.analysis.electricity_disagg_diagnostics.dispersion import ChainedDispersion
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
