"""Figures for monetary disagg implementation report (Decisions 2–5)."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from bedrock.analysis.electricity.monetary_disagg.figure_data import (
    ELEC,
    SECTOR_SHORT,
    commodity_row_block,
    industry_column_block,
    make_intersection_block,
    production_disaggregated,
    use_intersection_block,
    weight_summary,
)

OUTPUT_DIR = Path(__file__).resolve().parent / 'output'

SECTOR_LABELS = ['Gen\n221110', 'Trans\n221121', 'Dist\n221122']
SECTOR_COLORS = {'221110': '#2ca02c', '221121': '#ff7f0e', '221122': '#1f77b4'}


def _save(fig: Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    return path


def _heatmap_matrix(
    ax: Axes,
    matrix: pd.DataFrame,
    *,
    title: str,
    vmax: float | None = None,
) -> object:
    data = matrix.astype(float).to_numpy()
    scale = 1e9
    data_b = data / scale
    vmax_b = (vmax / scale if vmax is not None else float(np.nanmax(data_b))) or 1.0
    im = ax.imshow(data_b, cmap='YlOrRd', vmin=0.0, vmax=vmax_b)
    n = data.shape[0]
    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(SECTOR_LABELS[:n], fontsize=7)
    ax.set_yticklabels(SECTOR_LABELS[:n], fontsize=7)
    ax.set_xlabel('Industry column', fontsize=8)
    ax.set_ylabel('Commodity row', fontsize=8)
    ax.set_title(title, fontsize=9, pad=8)
    for i in range(n):
        for j in range(n):
            val = data_b[i, j]
            if val <= 0:
                continue
            ax.text(
                j,
                i,
                f'{val:.1f}',
                ha='center',
                va='center',
                fontsize=7,
                color='black' if val < vmax_b * 0.55 else 'white',
            )
    return im


def plot_decision2_make_intersection() -> Figure:
    v, _u, _ui, _va, _y = production_disaggregated()
    matrix = make_intersection_block(v)
    fig, ax = plt.subplots(figsize=(4.2, 3.8))
    _heatmap_matrix(
        ax,
        matrix,
        title='Decision 2 — Make diagonal (UGO305 GO weights)',
    )
    fig.suptitle(
        'Production Make table after step 1 ($B)',
        fontsize=11,
        weight='bold',
        y=1.02,
    )
    return fig


def plot_decision3_use_intersection() -> Figure:
    _v, udom, uimp, _va, _y = production_disaggregated()
    matrix = use_intersection_block(udom, uimp)
    fig, ax = plt.subplots(figsize=(4.2, 3.8))
    _heatmap_matrix(
        ax,
        matrix,
        title='Decision 3 — Use intersection (Purchased Power + T/D)',
    )
    fig.suptitle(
        'Production Use intersection after step 2 ($B)',
        fontsize=11,
        weight='bold',
        y=1.02,
    )
    return fig


def plot_decision4_use_columns() -> Figure:
    _v, udom, uimp, va, _y = production_disaggregated()
    block = industry_column_block(udom, uimp)
    fig, ax = plt.subplots(figsize=(5.5, 4.2))
    data = block.to_numpy() / 1e9
    im = ax.imshow(data, cmap='Blues', aspect='auto')
    ax.set_xticks(range(len(ELEC)))
    ax.set_xticklabels(SECTOR_LABELS, fontsize=7)
    ax.set_yticks(range(len(block.index)))
    ax.set_yticklabels(block.index, fontsize=7)
    ax.set_xlabel('Electricity industry column', fontsize=8)
    ax.set_ylabel('Purchaser commodity row', fontsize=8)
    ax.set_title(
        'Decision 4 — Industry column split (selected rows, $B)',
        fontsize=9,
        pad=8,
    )
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if val <= 0:
                continue
            ax.text(j, i, f'{val:.1f}', ha='center', va='center', fontsize=6)
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04).set_label('$B', fontsize=8)
    va_cols = va[ELEC].sum(axis=0) / 1e9
    for j, code in enumerate(ELEC):
        ax.text(
            j,
            -0.85,
            f'VA col\n{float(va_cols[code]):.1f}B',
            ha='center',
            va='top',
            fontsize=7,
            color=SECTOR_COLORS[code],
        )
    fig.suptitle(
        'Fuel rows → generation only; other rows split by w_go; VA residual',
        fontsize=10,
        weight='bold',
        y=1.05,
    )
    fig.subplots_adjust(bottom=0.18)
    return fig


def plot_decision5_row_y() -> Figure:
    _v, udom, uimp, _va, y = production_disaggregated()
    block = commodity_row_block(udom, uimp, y)
    weights = weight_summary()

    fig = plt.figure(figsize=(10, 4.2))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.4, 1], wspace=0.35)
    ax_h = fig.add_subplot(gs[0, 0])
    ax_w = fig.add_subplot(gs[0, 1])

    data = block.to_numpy() / 1e9
    im = ax_h.imshow(data, cmap='YlOrRd', aspect='auto')
    ax_h.set_xticks(range(len(block.columns)))
    ax_h.set_xticklabels(block.columns, rotation=35, ha='right', fontsize=7)
    ax_h.set_yticks(range(len(ELEC)))
    ax_h.set_yticklabels(SECTOR_LABELS, fontsize=7)
    ax_h.set_ylabel('Electricity commodity row', fontsize=8)
    ax_h.set_xlabel('Purchaser industry / FD column', fontsize=8)
    ax_h.set_title('Decision 5 — Commodity row + Y split ($B)', fontsize=9)
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if val <= 0:
                continue
            ax_h.text(j, i, f'{val:.1f}', ha='center', va='center', fontsize=6)
    fig.colorbar(im, ax=ax_h, fraction=0.046, pad=0.04).set_label('$B', fontsize=8)

    w = weights[['w_go (steps 1 & 3)', 'w_row (step 4 & Y)']].astype(float)
    w_go_vals = w['w_go (steps 1 & 3)'].to_numpy(dtype=float)
    w_row_vals = w['w_row (step 4 & Y)'].to_numpy(dtype=float)
    x = np.arange(len(ELEC))
    width = 0.35
    for i, code in enumerate(ELEC):
        ax_w.bar(
            i - width / 2,
            w_go_vals[i],
            width,
            label='w_go' if i == 0 else '',
            color='#aaaaaa',
        )
        ax_w.bar(
            i + width / 2,
            w_row_vals[i],
            width,
            label='w_row' if i == 0 else '',
            color=SECTOR_COLORS[code],
        )
    ax_w.set_xticks(x)
    ax_w.set_xticklabels([SECTOR_SHORT[c] for c in ELEC], fontsize=8)
    ax_w.set_ylabel('Weight share', fontsize=8)
    ax_w.set_title('Compensating row weights', fontsize=9)
    ax_w.set_ylim(0, 1)
    ax_w.legend(fontsize=8)
    ax_w.grid(axis='y', alpha=0.3)

    fig.suptitle(
        'Production step 4 / Y — w_row preserves UGO305 totals on aggregate allocation',
        fontsize=10,
        weight='bold',
    )
    return fig


def build_report_figures(*, output_dir: Path | None = None) -> dict[str, Path]:
    out = output_dir or OUTPUT_DIR
    return {
        'decision2_make': _save(
            plot_decision2_make_intersection(), out / 'figure_d2_make_intersection.png'
        ),
        'decision3_use': _save(
            plot_decision3_use_intersection(), out / 'figure_d3_use_intersection.png'
        ),
        'decision4_columns': _save(
            plot_decision4_use_columns(), out / 'figure_d4_use_columns.png'
        ),
        'decision5_row_y': _save(plot_decision5_row_y(), out / 'figure_d5_row_y.png'),
    }


if __name__ == '__main__':
    import warnings

    from bedrock.analysis.electricity.disaggregation_matrices import (
        assert_disaggregation_export_config,
    )
    from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

    warnings.filterwarnings('ignore')
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(
        'test_usa_config_waste_disagg_electricity_disaggregation.yaml'
    )
    assert_disaggregation_export_config()
    paths = build_report_figures()
    for name, path in paths.items():
        print(f'Wrote {name}: {path}')
