"""Matplotlib figures for methods #85 Decisions 3 and 5 (analysis-only)."""

from __future__ import annotations

from pathlib import Path
from typing import cast

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from bedrock.analysis.electricity.d_85.figure_data import (
    ELEC_CODES,
    FIGURE_C_PANELS,
    FIGURE_D_SCENARIOS,
    SECTOR_SHORT,
    market_clearing_gaps_table,
    step2_intersection_matrix,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'

SECTOR_COLORS: dict[str, str] = {
    '221110': '#2ca02c',
    '221121': '#ff7f0e',
    '221122': '#1f77b4',
}

FIGURE_A_NAME = 'figure_a_pr3_scenario_map.png'
FIGURE_C_NAME = 'figure_c_step2_intersection_matrices.png'
FIGURE_D_NAME = 'figure_d_market_clearing_gaps.png'


def _save(fig: Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    return path


def plot_figure_a_pr3_scenario_map(*, ax: Axes | None = None) -> Figure:
    """PR3 pipeline with Decision 3 / 5 step overrides highlighted."""
    if ax is None:
        fig, ax = plt.subplots(figsize=(11, 3.2))
    else:
        fig = cast(Figure, ax.figure)

    ax.set_xlim(0, 10)
    ax.set_ylim(0, 4)
    ax.axis('off')

    steps = [
        (1.0, '1 — Make\nintersection', 'UGO305', '#e8e8e8', '#666666'),
        (3.1, '2 — Use\nintersection', '3×3 block', '#fdebd0', '#333333'),
        (5.2, '3 — Industry\ncolumns + VA', 'UGO305', '#e8e8e8', '#666666'),
        (7.3, '4 — Commodity\nrow / Y', 'UGO305 / price tilt', '#e8e8e8', '#666666'),
    ]
    for x, title, subtitle, face, edge in steps:
        patch = FancyBboxPatch(
            (x, 1.4),
            1.6,
            1.4,
            boxstyle='round,pad=0.05,rounding_size=0.08',
            linewidth=1.5,
            edgecolor=edge,
            facecolor=face,
        )
        ax.add_patch(patch)
        ax.text(
            x + 0.8, 2.35, title, ha='center', va='center', fontsize=9, weight='bold'
        )
        ax.text(x + 0.8, 1.85, subtitle, ha='center', va='center', fontsize=8)

    for x0, x1 in ((2.6, 3.1), (4.7, 5.2), (6.8, 7.3)):
        ax.add_patch(
            FancyArrowPatch(
                (x0, 2.1),
                (x1, 2.1),
                arrowstyle='-|>',
                mutation_scale=12,
                linewidth=1.2,
                color='#444444',
            )
        )

    d3_box = FancyBboxPatch(
        (2.55, 0.15),
        1.7,
        0.75,
        boxstyle='round,pad=0.04,rounding_size=0.06',
        linewidth=1.5,
        edgecolor='#c0392b',
        facecolor='#fadbd8',
    )
    ax.add_patch(d3_box)
    ax.text(
        3.4,
        0.52,
        'Decision 3\nTable 8.3 weights',
        ha='center',
        va='center',
        fontsize=8,
        color='#922b21',
        weight='bold',
    )

    d5_box = FancyBboxPatch(
        (7.15, 0.15),
        1.7,
        0.75,
        boxstyle='round,pad=0.04,rounding_size=0.06',
        linewidth=1.5,
        edgecolor='#1f618d',
        facecolor='#d6eaf8',
    )
    ax.add_patch(d5_box)
    ax.text(
        8.0,
        0.52,
        'Decision 5\nTable 2.4 price tilt',
        ha='center',
        va='center',
        fontsize=8,
        color='#1a5276',
        weight='bold',
    )

    ax.text(
        0.2,
        3.55,
        'PR3 electricity disaggregation — which step each scenario overrides',
        fontsize=11,
        weight='bold',
        ha='left',
    )
    ax.text(
        5.0,
        0.05,
        'baseline: UGO305 at all steps  |  D3: step 2 only  |  D5: step 4 only',
        fontsize=8,
        ha='center',
        color='#555555',
    )
    return fig


def plot_figure_c_step2_intersection_matrices() -> Figure:
    """Heatmaps of the 3×3 Use-intersection block for Decision 3 scenarios."""
    matrices = [
        (label, step2_intersection_matrix(scenario_id))
        for scenario_id, label in FIGURE_C_PANELS
    ]
    vmax = max(float(m.values.max()) for _, m in matrices)

    fig, axes = plt.subplots(1, len(matrices), figsize=(3.2 * len(matrices), 3.6))
    if len(matrices) == 1:
        axes = [axes]

    sector_labels = ['Gen\n221110', 'Trans\n221121', 'Dist\n221122']

    for ax, (title, matrix) in zip(axes, matrices, strict=True):
        data = matrix.reindex(index=ELEC_CODES, columns=ELEC_CODES).to_numpy() / 1e9
        im = ax.imshow(data, cmap='YlOrRd', vmin=0.0, vmax=vmax / 1e9)
        ax.set_xticks(range(3))
        ax.set_yticks(range(3))
        ax.set_xticklabels(sector_labels, fontsize=7)
        ax.set_yticklabels(sector_labels, fontsize=7)
        ax.set_xlabel('Industry column', fontsize=8)
        ax.set_ylabel('Commodity row', fontsize=8)
        ax.set_title(title, fontsize=9, pad=8)

        for i in range(3):
            for j in range(3):
                val = data[i, j]
                if val <= 0:
                    continue
                ax.text(
                    j,
                    i,
                    f'{val:.1f}',
                    ha='center',
                    va='center',
                    fontsize=7,
                    color='black' if val < vmax / 1e9 * 0.55 else 'white',
                )

    fig.suptitle(
        'Figure C — Step-2 Use intersection (221100 block split), $B',
        fontsize=11,
        weight='bold',
        y=1.02,
    )
    cbar = fig.colorbar(im, ax=axes, fraction=0.025, pad=0.02)
    cbar.set_label('Cell value ($B)', fontsize=8)
    fig.subplots_adjust(top=0.82, wspace=0.35)
    return fig


def plot_figure_d_market_clearing_gaps(
    d3: dict[str, DisaggScenarioResult],
    d5: dict[str, DisaggScenarioResult],
) -> Figure:
    """Diverging grouped bars: (U row + Y row) − q by scenario and child sector."""
    gaps = market_clearing_gaps_table(d3, d5)
    labels = [label for _, _, label in FIGURE_D_SCENARIOS]
    scenario_ids = [sid for sid, _, _ in FIGURE_D_SCENARIOS]
    metrics_only = {
        sid: bool(d3[sid].metrics_only if sid in d3 else d5[sid].metrics_only)
        for sid in scenario_ids
    }

    x = np.arange(len(labels))
    width = 0.24
    sectors = ['221110', '221121', '221122']

    fig, (ax_top, ax_main) = plt.subplots(
        2,
        1,
        figsize=(12, 6),
        gridspec_kw={'height_ratios': [1, 4], 'hspace': 0.08},
    )

    baseline_gap = gaps.loc[gaps['scenario_id'] == 'baseline']
    for i, sector in enumerate(sectors):
        val_m = float(
            baseline_gap.loc[
                baseline_gap['sector'] == sector, 'market_clearing_gap'
            ].iloc[0]
        )
        offset = (i - 1) * width
        ax_top.bar(
            0 + offset,
            val_m / 1e6,
            width,
            color=SECTOR_COLORS[sector],
            label=SECTOR_SHORT.get(sector, sector),
            edgecolor='white',
            linewidth=0.5,
        )

    ax_top.set_xlim(-0.6, len(labels) - 0.4)
    ax_top.set_xticks([])
    ax_top.set_ylabel('Gap ($M)', fontsize=8)
    ax_top.set_title('baseline (noise scale)', fontsize=8, loc='left')
    ax_top.axhline(0, color='#888888', linewidth=0.8)
    ax_top.grid(axis='y', alpha=0.3)

    for i, sector in enumerate(sectors):
        sector_gaps = gaps.loc[gaps['sector'] == sector]
        vals = [
            float(
                sector_gaps.loc[
                    sector_gaps['scenario_id'] == sid, 'market_clearing_gap_b'
                ].iloc[0]
            )
            for sid in scenario_ids
        ]
        offset = (i - 1) * width
        bars = ax_main.bar(
            x + offset,
            vals,
            width,
            color=SECTOR_COLORS[sector],
            label=SECTOR_SHORT.get(sector, sector),
            edgecolor='white',
            linewidth=0.5,
        )
        for bar, sid in zip(bars, scenario_ids, strict=True):
            if metrics_only[sid]:
                bar.set_hatch('///')
                bar.set_edgecolor('#555555')

    ax_main.axhline(0, color='#444444', linewidth=0.9)
    ax_main.set_xticks(x)
    ax_main.set_xticklabels(labels, rotation=25, ha='right', fontsize=8)
    ax_main.set_ylabel('Market-clearing gap ($B)\n(Use row + Y row − q)', fontsize=9)
    ax_main.grid(axis='y', alpha=0.3)
    ax_main.legend(loc='upper left', fontsize=8, ncol=3)

    ax_main.text(
        0.99,
        0.02,
        '/// = metrics_only (VA step failed; gaps shown for diagnostics)',
        transform=ax_main.transAxes,
        ha='right',
        va='bottom',
        fontsize=7,
        color='#555555',
    )

    fig.suptitle(
        'Figure D — Commodity market clearing by scenario (Decisions 3 & 5)',
        fontsize=11,
        weight='bold',
    )
    fig.subplots_adjust(hspace=0.12, top=0.92)
    return fig


def build_decision_figures(
    d3: dict[str, DisaggScenarioResult],
    d5: dict[str, DisaggScenarioResult],
    *,
    output_dir: Path | None = None,
) -> dict[str, Path]:
    """Write figures A, C, and D to ``output_dir`` (default: d_85/output/)."""
    out = output_dir or OUTPUT_DIR
    paths = {
        'figure_a': _save(plot_figure_a_pr3_scenario_map(), out / FIGURE_A_NAME),
        'figure_c': _save(
            plot_figure_c_step2_intersection_matrices(), out / FIGURE_C_NAME
        ),
        'figure_d': _save(
            plot_figure_d_market_clearing_gaps(d3, d5), out / FIGURE_D_NAME
        ),
    }
    return paths


if __name__ == '__main__':
    import warnings

    from bedrock.analysis.electricity.d_85.disagg_scenarios import (
        run_decision3_scenarios,
        run_decision5_scenarios,
    )
    from bedrock.analysis.electricity.disaggregation_matrices import (
        assert_disaggregation_export_config,
    )
    from bedrock.utils.config.usa_config import set_global_usa_config

    warnings.filterwarnings('ignore')
    set_global_usa_config(
        '2025_usa_cornerstone_full_model_electricity_disaggregation.yaml'
    )
    assert_disaggregation_export_config()
    written = build_decision_figures(
        run_decision3_scenarios(), run_decision5_scenarios()
    )
    for name, path in written.items():
        print(f'Wrote {name}: {path}')
