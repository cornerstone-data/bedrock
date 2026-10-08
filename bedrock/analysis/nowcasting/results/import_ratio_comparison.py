"""Imports in A across A-matrix methods: totals, commodity import proportions, cell structure.

Compares the Cornerstone nowcast (live, 2017-2024) with the May 2026 cached
approach matrices (``A_{approach}_{year}.parquet`` with ``dom``/``imp`` blocks
and ``q_{approach}_{year}.parquet``) and with BEA's published summary Use
tables as the outside reference.

Per (approach, year), with ``q`` as column weights:

- intermediate imports ``M = sum_ij Aimp_ij q_j`` and their share of all
  intermediate inputs ``M / sum_ij A_ij q_j``;
- commodity import proportion ``r_i = sum_j Aimp_ij q_j / sum_j A_ij q_j``;
- within-row dispersion of the cell import ratio ``Aimp_ij / A_ij`` (weighted
  by ``A_ij q_j``), zero when imports are allocated proportionally.

Figures (gitignored, next to this file):

- ``import_share_of_intermediate_inputs_by_year.png``
- ``import_total_indexed_2017_by_year.png``
- ``commodity_import_proportion_nowcast_vs_others_2023.png``
- ``commodity_import_proportion_abs_diff_vs_nowcast_2023.png``
- ``cell_import_ratio_within_row_dispersion_2023.png``

::

    uv run python -m bedrock.analysis.nowcasting.results.import_ratio_comparison
"""

from __future__ import annotations

import argparse
import typing as ta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import aq_from_live_config
from bedrock.analysis.nowcasting.results.a_method_figures_with_nowcast import (
    APPROACH_COLORS as _DECK_COLORS,
)
from bedrock.analysis.nowcasting.results.a_method_figures_with_nowcast import (
    DEFAULT_RESULTS_DIR,
    NOWCAST,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl

OUT_DIR = Path(__file__).resolve().parent

YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024)
FOCUS_YEAR = 2023

# Cached deck approaches (May 2026 parquets) in plot order, then the nowcast.
CACHED_APPROACHES: tuple[str, ...] = (
    'useeio',
    'ceda_default',
    'summary_tables',
    'commodity_price_index',
    'useeio_nowcast',
)
APPROACH_ORDER: tuple[str, ...] = (*CACHED_APPROACHES, NOWCAST)
APPROACH_COLORS: dict[str, str] = {
    **_DECK_COLORS,
    'useeio': '#7f7f7f',
    'ceda_default': '#bcbd22',
}
BEA_LABEL = 'BEA summary Use (published)'


class ImportView(ta.NamedTuple):
    """One (approach, year): A blocks and the column weights."""

    Adom: pd.DataFrame
    Aimp: pd.DataFrame
    q: pd.Series


def main(*, results_dir: Path = DEFAULT_RESULTS_DIR) -> None:
    setup_mpl()
    views = _collect_views(results_dir)
    metrics = _aggregate_metrics(views)
    bea = _bea_summary_reference()
    metrics.to_csv(OUT_DIR / 'import_metrics_by_approach_year.csv', index=False)
    bea.to_csv(OUT_DIR / 'import_metrics_bea_summary_by_year.csv', index=False)
    print(
        metrics.pivot(index='year', columns='approach', values='import_share')
        .round(4)
        .to_string()
    )
    print('BEA summary intermediate import share:')
    print(bea.set_index('year')['import_share'].round(4).to_string())

    _plot_share_by_year(metrics, bea)
    _plot_total_indexed(metrics, bea)

    focus = {a: v for (a, y), v in views.items() if y == FOCUS_YEAR}
    props = pd.DataFrame({a: _commodity_import_proportion(v) for a, v in focus.items()})
    props.rename_axis('sector').to_csv(
        OUT_DIR / f'commodity_import_proportion_{FOCUS_YEAR}.csv'
    )
    _plot_proportion_scatter(props, focus)
    _plot_proportion_abs_diff(props)
    _plot_within_row_dispersion(focus)


# --- data ---------------------------------------------------------------------


def _collect_views(results_dir: Path) -> dict[tuple[str, int], ImportView]:
    views: dict[tuple[str, int], ImportView] = {}
    for approach in CACHED_APPROACHES:
        for year in YEARS:
            a_path = results_dir / f'A_{approach}_{year}.parquet'
            q_path = results_dir / f'q_{approach}_{year}.parquet'
            if not (a_path.exists() and q_path.exists()):
                continue
            stacked = pd.read_parquet(a_path)
            q = pd.read_parquet(q_path).iloc[:, 0].astype(float)
            views[(approach, year)] = ImportView(
                Adom=pd.DataFrame(stacked.loc['dom']).astype(float),
                Aimp=pd.DataFrame(stacked.loc['imp']).astype(float),
                q=q,
            )
    for year in YEARS:
        aq, vintage = aq_from_live_config(year)
        print(f'  {NOWCAST} {year}: MUT vintage {vintage}')
        views[(NOWCAST, year)] = ImportView(
            Adom=pd.DataFrame(aq.Adom).astype(float),
            Aimp=pd.DataFrame(aq.Aimp).astype(float),
            q=pd.Series(aq.scaled_q, dtype=float),
        )
    return views


def _weighted_blocks(v: ImportView) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dollar flows ``Aimp_ij q_j`` and ``A_ij q_j`` on the common sector set."""
    cols = v.Adom.columns.intersection(v.q.index)
    q = v.q.reindex(cols)
    imp = v.Aimp.loc[:, cols].mul(q, axis=1)
    tot = (v.Adom.loc[:, cols] + v.Aimp.loc[:, cols]).mul(q, axis=1)
    return imp, tot


def _aggregate_metrics(views: dict[tuple[str, int], ImportView]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (approach, year), v in sorted(
        views.items(), key=lambda kv: (kv[0][1], kv[0][0])
    ):
        imp, tot = _weighted_blocks(v)
        m = float(imp.to_numpy().sum())
        t = float(tot.to_numpy().sum())
        rows.append(
            {
                'approach': approach,
                'year': year,
                'intermediate_imports': m,
                'intermediate_inputs': t,
                'import_share': m / t if t else np.nan,
            }
        )
    df = pd.DataFrame(rows)
    base = df[df['year'] == YEARS[0]].set_index('approach')['intermediate_imports']
    df['imports_index_2017'] = df['intermediate_imports'] / df['approach'].map(base)
    return df


def _bea_summary_reference() -> pd.DataFrame:
    """Intermediate import share and indexed import total from BEA summary Use."""
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        load_summary_Uimp_usa,
        load_summary_Utot_usa,
    )

    rows: list[dict[str, object]] = []
    for year in YEARS:
        ui = load_summary_Uimp_usa(year)
        ut = load_summary_Utot_usa(year)
        m = float(ui.to_numpy().sum())
        rows.append(
            {
                'year': year,
                'intermediate_imports': m,
                'import_share': m / float(ut.to_numpy().sum()),
            }
        )
    df = pd.DataFrame(rows)
    df['imports_index_2017'] = (
        df['intermediate_imports'] / df['intermediate_imports'].iloc[0]
    )
    return df


def _commodity_import_proportion(v: ImportView) -> pd.Series:
    imp, tot = _weighted_blocks(v)
    return (imp.sum(axis=1) / tot.sum(axis=1).replace(0, np.nan)).rename('r')


def _within_row_dispersion(v: ImportView) -> pd.Series:
    """Weighted std of ``Aimp_ij / A_ij`` across j, per commodity row i."""
    imp, tot = _weighted_blocks(v)
    ratio = (v.Aimp / (v.Adom + v.Aimp).replace(0, np.nan)).reindex(
        index=tot.index, columns=tot.columns
    )
    w = tot.where(tot > 0, 0.0)
    wsum = w.sum(axis=1).replace(0, np.nan)
    mean = (ratio.fillna(0) * w).sum(axis=1) / wsum
    var = (((ratio.sub(mean, axis=0)) ** 2).fillna(0) * w).sum(axis=1) / wsum
    return pd.Series(
        np.sqrt(var.to_numpy(dtype=float)), index=var.index, name='within_row_std'
    )


# --- figures ------------------------------------------------------------------


def _label(a: str) -> str:
    return 'bea_2017_benchmark (useeio)' if a == 'useeio' else a


def _plot_share_by_year(metrics: pd.DataFrame, bea: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    for a in APPROACH_ORDER:
        sub = metrics[metrics['approach'] == a].sort_values('year')
        if sub.empty:
            continue
        ax.plot(
            sub['year'],
            100 * sub['import_share'],
            marker='o',
            color=APPROACH_COLORS.get(a),
            lw=2.4 if a == NOWCAST else 1.4,
            label=_label(a),
        )
    ax.plot(
        bea['year'],
        100 * bea['import_share'],
        color='black',
        ls='--',
        marker='s',
        label=BEA_LABEL,
    )
    ax.set_xticks(list(YEARS))
    ax.set_ylabel('imports, % of intermediate inputs')
    ax.set_xlabel('year')
    ax.set_title('Intermediate import share by A-matrix method')
    ax.grid(axis='y', color='gray', ls=':', lw=0.8, alpha=0.8)
    ax.legend(fontsize=8, loc='best')
    fig.tight_layout()
    fig.savefig(
        OUT_DIR / 'import_share_of_intermediate_inputs_by_year.png',
        dpi=150,
        bbox_inches='tight',
    )
    plt.close(fig)


def _plot_total_indexed(metrics: pd.DataFrame, bea: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    for a in APPROACH_ORDER:
        sub = metrics[metrics['approach'] == a].sort_values('year')
        if sub.empty:
            continue
        ax.plot(
            sub['year'],
            sub['imports_index_2017'],
            marker='o',
            color=APPROACH_COLORS.get(a),
            lw=2.4 if a == NOWCAST else 1.4,
            label=_label(a),
        )
    ax.plot(
        bea['year'],
        bea['imports_index_2017'],
        color='black',
        ls='--',
        marker='s',
        label=BEA_LABEL,
    )
    ax.axhline(1.0, color='black', lw=0.8, ls='--', alpha=0.5)
    ax.set_xticks(list(YEARS))
    ax.set_ylabel('nominal intermediate imports, 2017 = 1')
    ax.set_xlabel('year')
    ax.set_title('Total intermediate imports implied by A, indexed to 2017')
    ax.grid(axis='y', color='gray', ls=':', lw=0.8, alpha=0.8)
    ax.legend(fontsize=8, loc='best')
    fig.tight_layout()
    fig.savefig(
        OUT_DIR / 'import_total_indexed_2017_by_year.png', dpi=150, bbox_inches='tight'
    )
    plt.close(fig)


def _plot_proportion_scatter(props: pd.DataFrame, focus: dict[str, ImportView]) -> None:
    others = [a for a in ('ceda_default', 'useeio_nowcast') if a in props.columns]
    fig, axes = plt.subplots(
        1, len(others), figsize=(6.2 * len(others), 6.0), squeeze=False
    )
    imp_nc, _ = _weighted_blocks(focus[NOWCAST])
    size = imp_nc.sum(axis=1)
    for ax, other in zip(axes[0], others):
        df = props[[other, NOWCAST]].dropna()
        s = size.reindex(df.index).fillna(0)
        ax.scatter(
            df[other],
            df[NOWCAST],
            s=8 + 120 * (s / s.max()),
            alpha=0.55,
            color=APPROACH_COLORS[NOWCAST],
            edgecolor='none',
        )
        ax.plot([0, 1], [0, 1], color='black', lw=0.8, ls='--')
        movers = (
            (df[NOWCAST] - df[other]).abs().sort_values(ascending=False).head(8).index
        )
        for code in movers:
            ax.annotate(
                code,
                (df.loc[code, other], df.loc[code, NOWCAST]),
                fontsize=7,
                xytext=(3, 3),
                textcoords='offset points',
            )
        ax.set_xlabel(_label(other))
        ax.set_ylabel(NOWCAST)
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.grid(color='gray', ls=':', lw=0.8, alpha=0.6)
    fig.suptitle(
        f'Commodity import proportion (imports / total use of i), {FOCUS_YEAR}; '
        'marker size = nowcast import $',
        y=1.02,
    )
    fig.tight_layout()
    fig.savefig(
        OUT_DIR / f'commodity_import_proportion_nowcast_vs_others_{FOCUS_YEAR}.png',
        dpi=150,
        bbox_inches='tight',
    )
    plt.close(fig)


def _plot_proportion_abs_diff(props: pd.DataFrame) -> None:
    approaches = [a for a in CACHED_APPROACHES if a in props.columns]
    data = [
        (props[a] - props[NOWCAST]).abs().dropna().to_numpy(dtype=float)
        for a in approaches
    ]
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    parts = ax.violinplot(
        data,
        positions=list(range(1, len(approaches) + 1)),
        showmedians=True,
        widths=0.8,
    )
    for body, a in zip(ta.cast(list[ta.Any], parts['bodies']), approaches):
        body.set_facecolor(APPROACH_COLORS.get(a, '#7f7f7f'))
        body.set_edgecolor(APPROACH_COLORS.get(a, '#7f7f7f'))
        body.set_alpha(0.85)
    for key in ('cmedians', 'cmaxes', 'cmins', 'cbars'):
        if key in parts:
            parts[key].set_color('black')
            parts[key].set_linewidth(1.0)
    ax.set_xticks(list(range(1, len(approaches) + 1)))
    ax.set_xticklabels([_label(a).replace(' ', '\n') for a in approaches], fontsize=8)
    ax.set_ylabel('abs. difference in import proportion')
    ax.set_title(f'|r_method - r_{NOWCAST}| per commodity, {FOCUS_YEAR}')
    ax.grid(axis='y', color='gray', ls=':', lw=0.8, alpha=0.8)
    for i, d in enumerate(data, start=1):
        ax.text(
            i,
            ax.get_ylim()[1] * 0.90,
            f'median {np.median(d):.3f}\np90 {np.quantile(d, 0.9):.3f}',
            ha='center',
            va='top',
            fontsize=8,
            bbox=dict(
                boxstyle='round,pad=0.2', facecolor='white', alpha=0.8, edgecolor='none'
            ),
        )
    fig.tight_layout()
    fig.savefig(
        OUT_DIR / f'commodity_import_proportion_abs_diff_vs_nowcast_{FOCUS_YEAR}.png',
        dpi=150,
        bbox_inches='tight',
    )
    plt.close(fig)


def _plot_within_row_dispersion(focus: dict[str, ImportView]) -> None:
    approaches = [a for a in APPROACH_ORDER if a in focus]
    fig, axes = plt.subplots(
        1,
        len(approaches),
        figsize=(4.2 * len(approaches), 4.6),
        sharey=True,
        squeeze=False,
    )
    bins = np.linspace(0, 0.5, 51)
    rows: dict[str, pd.Series] = {}
    for ax, a in zip(axes[0], approaches):
        d = _within_row_dispersion(focus[a]).dropna()
        rows[a] = d
        ax.hist(
            d.clip(upper=0.5).to_numpy(dtype=float),
            bins=bins,
            color=APPROACH_COLORS.get(a, '#7f7f7f'),
            alpha=0.85,
        )
        ax.set_title(_label(a), fontsize=10)
        ax.text(
            0.97,
            0.95,
            f'median {d.median():.3f}\nmax {d.max():.2f}',
            transform=ax.transAxes,
            ha='right',
            va='top',
            fontsize=8,
        )
        ax.set_xlabel('within-row std of Aimp/A')
        ax.grid(axis='y', color='gray', ls=':', lw=0.8, alpha=0.6)
    axes[0][0].set_ylabel('commodities')
    fig.suptitle(
        f'Cell import ratio dispersion across users of each commodity, {FOCUS_YEAR} '
        '(0 = imports allocated proportionally to use)'
    )
    fig.tight_layout()
    fig.savefig(
        OUT_DIR / f'cell_import_ratio_within_row_dispersion_{FOCUS_YEAR}.png',
        dpi=150,
        bbox_inches='tight',
    )
    plt.close(fig)
    pd.DataFrame(rows).rename_axis('sector').to_csv(
        OUT_DIR / f'cell_import_ratio_within_row_std_{FOCUS_YEAR}.csv'
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-dir', type=Path, default=DEFAULT_RESULTS_DIR)
    args = parser.parse_args()
    main(results_dir=args.results_dir)
