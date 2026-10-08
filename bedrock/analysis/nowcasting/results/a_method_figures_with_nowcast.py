"""Deck figures for the A-matrix method comparison, with the Cornerstone nowcast added.

Two figures from the May 2026 diagnostics review gain a fourth method,
``cornerstone_nowcast`` (after-redef NowcastMUT detail A, no scaling step):

1. Per-sector N % diff vs CEDA-US (v0) at 2023, one file per deck scenario.
   The three deck methods come from the cached ``ef_scatter_coords.parquet``.
   ``isolate_a_matrix``: the nowcast 2023 A replaces the A matrix while D is
   held to the isolate scenario's D (``N = D . L``), so only the A method
   moves. ``bundle_v0_2``: the deck methods bundled with the full v0.2 model,
   set against the full nowcast model's N (A, x and GHG all at 2023).
2. Signed year-over-year change in N, 2019 -> 2023, N deflated to 2023$.
   Deck methods come from the ``bundle_v0_2`` tabs of ``ef_comparison.xlsx``;
   the nowcast series is the full nowcast model per year.

Inputs are the untracked May 2026 caches under
``bedrock/analysis/a_matrix_time_series/output/results/`` (``--results-dir``).
Outputs land next to this file (gitignored).

::

    uv run python -m bedrock.analysis.nowcasting.results.a_method_figures_with_nowcast
"""

from __future__ import annotations

import argparse
import re
import typing as ta
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    aq_from_live_config,
    efs_from_live_config,
)
from bedrock.utils.math.formulas import compute_L_matrix
from bedrock.utils.validation.analysis.ef_hist_panels import (
    HIST_FONT_SCALE,
    draw_per_sector_pct_hist_panel,
)
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

OUT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = OUT_DIR.parents[1] / 'a_matrix_time_series' / 'output' / 'results'

NOWCAST = 'cornerstone_nowcast'
DECK_APPROACHES: tuple[str, ...] = (
    'summary_tables',
    'commodity_price_index',
    'useeio_nowcast',
)
APPROACH_ORDER: tuple[str, ...] = (*DECK_APPROACHES, NOWCAST)
APPROACH_COLORS: dict[str, str] = {
    'summary_tables': '#1f77b4',
    'commodity_price_index': '#2ca02c',
    'useeio_nowcast': '#ff7f0e',
    NOWCAST: '#d62728',
}

HIST_YEAR = 2023
REF_DOLLAR_YEAR = 2023
YOY_YEARS: tuple[int, ...] = (2019, 2020, 2021, 2022, 2023)
YOY_TRANSITIONS: tuple[tuple[int, int], ...] = tuple(
    (YOY_YEARS[i], YOY_YEARS[i + 1]) for i in range(len(YOY_YEARS) - 1)
)
# Drop sectors whose mean N sits below this percentile before the violins so
# near-zero denominators do not blow up the YoY ratios.
MIN_MEAN_PERCENTILE = 5

# Violin bodies use the same opacity as the shared histogram helper (0.85) so
# each method renders the same shade in both figures.
VIOLIN_ALPHA = 0.85

# Font sizes copied from the deck's histogram/violin scripts.
TITLE_FONTSIZE = 20
AXIS_LABEL_FONTSIZE = 11
SUPTITLE_FONTSIZE = 14
TICK_LABEL_FONTSIZE = 10

# ``compile_ef_diagnostics`` named workbook tabs
# ``{scenario}_{year}.0_{approach}__vs_{baseline}`` truncated to 31 chars.
_TAB_RE = re.compile(
    r'^(?P<scenario>[a-z0-9_]+?)_(?P<year>\d{4})(?:\.0)?_(?P<rest>.+)$'
)
_TAB_APPROACH_PREFIXES: tuple[tuple[str, str], ...] = (
    ('commodity_pr', 'commodity_price_index'),
    ('summary_tabl', 'summary_tables'),
    ('useeio_nowca', 'useeio_nowcast'),
)


def main(*, results_dir: Path = DEFAULT_RESULTS_DIR, skip_violin: bool = False) -> None:
    coords_path = results_dir / 'ef_scatter_coords.parquet'
    xlsx_path = results_dir / 'ef_comparison.xlsx'
    for p in (coords_path, xlsx_path):
        if not p.exists():
            raise FileNotFoundError(f'{p} not found; pass --results-dir')

    # --- Figure 1: N % diff histograms at 2023, two scenarios ----------------
    d_iso = _isolate_D(xlsx_path)
    aq, vintage = aq_from_live_config(HIST_YEAR)
    print(f'nowcast {HIST_YEAR}: MUT vintage {vintage}')
    n_iso = _n_from_fixed_D(d_iso, A=aq.Adom + aq.Aimp)
    n_full = efs_from_live_config(HIST_YEAR).N

    variants = (
        # deck scenario, its label, nowcast series, how the nowcast panel was built
        (
            'isolate_a_matrix',
            'isolate A-matrix method',
            n_iso,
            'nowcast 2023 A, isolate D',
        ),
        (
            'bundle_v0_2',
            'A-matrix method bundled with bedrock v0.2',
            n_full,
            'full nowcast 2023 model',
        ),
    )
    for scenario, scen_label, n_nc, nc_label in variants:
        deck = _deck_panels(coords_path, scenario)
        baseline = _deck_baseline(deck)
        rows: dict[str, pd.Series] = {
            a: _pct(deck[a]['y_approach'], deck[a]['x_baseline'])
            for a in DECK_APPROACHES
        }
        idx = n_nc.index.intersection(baseline.index)
        rows[NOWCAST] = _pct(n_nc.reindex(idx), baseline.reindex(idx))
        out = OUT_DIR / f'ef_pct_hist_{scenario}_ceda_N_with_nowcast.png'
        _hist_row(
            rows,
            suptitle=(
                'total EF (N) per-sector % diff distribution — vs CEDA-US (v0) '
                f'[{scen_label}] — year {HIST_YEAR}; {NOWCAST} = {nc_label}'
            ),
            out=out,
        )
        pd.DataFrame(rows).rename_axis('sector').to_csv(
            OUT_DIR / f'n_pct_diff_{scenario}_{HIST_YEAR}_with_nowcast.csv'
        )
        print(f'wrote {out.name}')
        for a, ser in rows.items():
            print(
                f'  {a:>24}: n={ser.size} median={100 * ser.median():+.1f}%  '
                f'p95(|.|)={100 * ser.abs().quantile(0.95):.1f}%'
            )

    if skip_violin:
        return

    # --- Figure 2: signed YoY violins ----------------------------------------
    panel = pd.concat(
        [_deck_bundle_panel(xlsx_path), _nowcast_bundle_panel()], ignore_index=True
    )
    per_sector = _yoy_per_sector(panel)
    per_sector.to_csv(OUT_DIR / 'n_yoy_per_sector_with_nowcast.csv', index=False)
    out = OUT_DIR / 'n_yoy_signed_violin_with_nowcast.png'
    _yoy_signed_violin_plot(per_sector, out, violin_ylim=(-40.0, 40.0))
    print(f'wrote {out.name}')
    cutoff = per_sector['mean_N'].abs().quantile(MIN_MEAN_PERCENTILE / 100)
    big = per_sector[per_sector['mean_N'].abs() >= cutoff]
    for a in APPROACH_ORDER:
        pooled = _pooled_signed_yoy(big, a) * 100
        if pooled.empty:
            continue
        print(
            f'  {a:>24}: pooled YoY median={pooled.median():+.1f}%  '
            f'mean|YoY|={pooled.abs().mean():.1f}%  '
            f'share |YoY|>5%={100 * (pooled.abs() > 5).mean():.0f}%'
        )


# --- data: isolate-A histogram ------------------------------------------------


def _deck_panels(coords_path: Path, scenario: str) -> dict[str, pd.DataFrame]:
    """``{approach: DataFrame[sector, x_baseline, y_approach]}`` for the deck methods.

    Same selection the deck used: the given scenario, baseline ``ceda``, N,
    and the latest tagged year (legacy untagged rows count as it).
    """
    coords = pd.read_parquet(coords_path)
    sub = coords[
        (coords['scenario'] == scenario)
        & (coords['baseline'] == 'ceda')
        & (coords['ef_kind'] == 'N')
    ].copy()
    year_str = sub['year'].fillna('').astype(str)
    sub = sub[(year_str == f'{HIST_YEAR}.0') | (year_str == '')]
    out: dict[str, pd.DataFrame] = {}
    for a in DECK_APPROACHES:
        df = sub[sub['approach'] == a].dropna(subset=['x_baseline', 'y_approach'])
        df = df[df['x_baseline'].abs() > 0]
        if df.empty:
            raise ValueError(f'no {scenario} rows for {a} at {HIST_YEAR}')
        out[a] = df.set_index('sector')[['x_baseline', 'y_approach']].astype(float)
    return out


def _deck_baseline(deck: dict[str, pd.DataFrame]) -> pd.Series:
    """CEDA-US (v0) N in 2023$, identical across the deck methods' tabs."""
    first = deck[DECK_APPROACHES[0]]['x_baseline']
    for a in DECK_APPROACHES[1:]:
        other = deck[a]['x_baseline'].reindex(first.index)
        if not np.allclose(first, other, equal_nan=True):
            raise ValueError(
                f'isolate baseline differs between {DECK_APPROACHES[0]} and {a}'
            )
    return first


def _isolate_D(xlsx_path: Path) -> pd.Series:
    """Direct EF (D) of the isolate scenario, from the summary_tables tab.

    D does not depend on the A method, so any isolate tab would do; the
    summary_tables tab is the reference.
    """
    tab = pd.read_excel(xlsx_path, sheet_name='isolate_a_matrix_summary_tables')
    d = pd.Series(
        pd.to_numeric(tab['D_new'], errors='coerce').to_numpy(),
        index=tab['index'].astype(str),
        name='D',
    )
    return d.dropna()


def _n_from_fixed_D(D: pd.Series, *, A: pd.DataFrame) -> pd.Series:
    """``N_j = sum_i D_i L_ij`` with ``L`` from ``A`` and ``D`` reindexed to A's sectors."""
    L = compute_L_matrix(A=A)
    sectors = L.index.intersection(D.index)
    dropped = sorted(set(D.index) - set(sectors))
    if dropped:
        print(f'  isolate D sectors without a nowcast A row (dropped): {dropped}')
    L_sq = L.loc[sectors, sectors]
    n = D.reindex(sectors).to_numpy(dtype=float) @ L_sq.to_numpy(dtype=float)
    return pd.Series(n, index=sectors, name='N')


def _pct(new: pd.Series, old: pd.Series) -> pd.Series:
    """Fractional diff ``(new - old) / |old|`` on the common index, zeros dropped."""
    idx = new.index.intersection(old.index)
    o = old.reindex(idx).astype(float)
    n = new.reindex(idx).astype(float)
    keep = o.abs() > 0
    return ((n[keep] - o[keep]) / o[keep].abs()).dropna()


def _hist_row(rows: dict[str, pd.Series], *, suptitle: str, out: Path) -> None:
    approaches = [a for a in APPROACH_ORDER if a in rows]
    n = len(approaches)
    fs = HIST_FONT_SCALE
    fig, axes = plt.subplots(1, n, figsize=(5.5 * n * fs, 6.0 * fs), squeeze=False)
    for ax, a in zip(axes[0], approaches):
        draw_per_sector_pct_hist_panel(
            ax,
            rows[a].to_numpy(dtype=float),
            title=a,
            color=APPROACH_COLORS.get(a, 'tab:blue'),
            font_scale=fs,
            ylabel='',
        )
        ax.tick_params(axis='both', labelsize=TICK_LABEL_FONTSIZE * fs)
        ax.set_xlabel('Percentage Diff (%)', fontsize=AXIS_LABEL_FONTSIZE * fs)
    axes[0][0].set_ylabel('sector count', fontsize=AXIS_LABEL_FONTSIZE * fs)
    fig.suptitle(suptitle, fontsize=SUPTITLE_FONTSIZE * fs, y=1.0)
    fig.tight_layout()
    fig.savefig(out, dpi=150, bbox_inches='tight')
    plt.close(fig)


# --- data: YoY violins --------------------------------------------------------


def _parse_tab(tab: str) -> tuple[str, int, str] | None:
    m = _TAB_RE.match(tab)
    if m is None:
        return None
    rest = m.group('rest')
    for prefix, approach in _TAB_APPROACH_PREFIXES:
        if rest.startswith(prefix):
            return m.group('scenario'), int(m.group('year')), approach
    return None


def _deck_bundle_panel(xlsx_path: Path) -> pd.DataFrame:
    """Long panel ``(approach, year, sector, N_new_ref)`` from the bundle_v0_2 tabs."""
    xls = pd.ExcelFile(xlsx_path)
    rows: list[pd.DataFrame] = []
    for tab in xls.sheet_names:
        parsed = _parse_tab(str(tab))
        if parsed is None or parsed[0] != 'bundle_v0_2':
            continue
        _scenario, year, approach = parsed
        df = pd.read_excel(xls, sheet_name=str(tab))
        rows.append(
            pd.DataFrame(
                {
                    'approach': approach,
                    'year': year,
                    'sector': df[df.columns[0]].astype(str),
                    'N_new_ref': pd.to_numeric(df['N_new_ref'], errors='coerce'),
                }
            )
        )
    if not rows:
        raise RuntimeError(f'no bundle_v0_2 tabs in {xlsx_path}')
    return pd.concat(rows, ignore_index=True)


def _nowcast_bundle_panel() -> pd.DataFrame:
    """Full nowcast model N per year, deflated to ``REF_DOLLAR_YEAR`` dollars."""
    rows: list[pd.DataFrame] = []
    for year in YOY_YEARS:
        efs = efs_from_live_config(year)
        n = efs.N
        if year != REF_DOLLAR_YEAR:
            n = inflation_adjust_ef_denom_to_new_base_year(
                n, new_base_year=REF_DOLLAR_YEAR, old_base_year=year
            )
        rows.append(
            pd.DataFrame(
                {
                    'approach': NOWCAST,
                    'year': year,
                    'sector': n.index.astype(str),
                    'N_new_ref': n.to_numpy(dtype=float),
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def _yoy_per_sector(panel: pd.DataFrame) -> pd.DataFrame:
    wide = panel.pivot_table(
        index=['approach', 'sector'],
        columns='year',
        values='N_new_ref',
        aggfunc='first',
    )
    for y in YOY_YEARS:
        if y not in wide.columns:
            wide[y] = np.nan
    wide = wide[list(YOY_YEARS)]
    wide.columns = pd.Index([f'N_{y}' for y in YOY_YEARS])
    wide = wide.reset_index()
    for y0, y1 in YOY_TRANSITIONS:
        wide[f'yoy_{y0}_{y1}'] = (wide[f'N_{y1}'] - wide[f'N_{y0}']) / wide[
            f'N_{y0}'
        ].abs()
    wide['mean_N'] = wide[[f'N_{y}' for y in YOY_YEARS]].mean(axis=1)
    return wide


def _pooled_signed_yoy(big: pd.DataFrame, approach: str) -> pd.Series:
    sub = big[big['approach'] == approach]
    return pd.concat([sub[f'yoy_{y0}_{y1}'] for y0, y1 in YOY_TRANSITIONS]).dropna()


def _clip_to_percentile(values: np.ndarray, lo: float, hi: float) -> np.ndarray:
    if values.size == 0:
        return values
    p_lo, p_hi = np.percentile(values, [lo, hi])
    return values[(values >= p_lo) & (values <= p_hi)]


def _style_violin(parts: dict[str, ta.Any], color: str) -> None:
    for body in ta.cast(list[ta.Any], parts['bodies']):
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(VIOLIN_ALPHA)
    for key in ('cmedians', 'cmaxes', 'cmins', 'cbars'):
        if key in parts:
            parts[key].set_color('black')
            parts[key].set_linewidth(1.0)
            parts[key].set_alpha(0.7)


def _yoy_signed_violin_plot(
    per_sector: pd.DataFrame, out_path: Path, *, violin_ylim: tuple[float, float]
) -> None:
    """Deck violin, two panels: pooled signed YoY per method; YoY per transition."""
    approaches = [a for a in APPROACH_ORDER if a in set(per_sector['approach'])]
    cutoff = per_sector['mean_N'].abs().quantile(MIN_MEAN_PERCENTILE / 100)
    big = per_sector[per_sector['mean_N'].abs() >= cutoff]

    lo, hi = violin_ylim
    span = int(max(abs(lo), abs(hi)))
    ticks = list(range(-span, span + 1, 10))
    tick_labels = [f'{t}%' for t in ticks]

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

    ax: Axes = axes[0]
    positions = list(range(1, len(approaches) + 1))
    pooled = [
        _clip_to_percentile(
            _pooled_signed_yoy(big, a).to_numpy(dtype=float) * 100, 1, 99
        )
        for a in approaches
    ]
    parts = ax.violinplot(
        pooled, positions=positions, showmedians=True, showextrema=True, widths=0.75
    )
    for body, a in zip(ta.cast(list[ta.Any], parts['bodies']), approaches):
        body.set_facecolor(APPROACH_COLORS[a])
        body.set_edgecolor(APPROACH_COLORS[a])
        body.set_alpha(VIOLIN_ALPHA)
    for key in ('cmedians', 'cmaxes', 'cmins', 'cbars'):
        if key in parts:
            parts[key].set_color('black')
            parts[key].set_linewidth(1.0)
            parts[key].set_alpha(0.7)
    ax.axhline(0, color='black', linewidth=0.8, alpha=0.6)
    ax.set_xticks(positions)
    ax.set_xticklabels([a.replace('_', '\n', 1) for a in approaches])
    ax.set_title('Pooled YoY (all sector-years)', fontsize=13)
    ax.set_ylabel('YoY (signed, 0 = no change)')
    ax.set_ylim(*violin_ylim)
    ax.set_yticks(ticks)
    ax.set_yticklabels(tick_labels)
    handles: list[ta.Any] = [
        Patch(facecolor=APPROACH_COLORS[a], alpha=VIOLIN_ALPHA, label=a)
        for a in approaches
    ]
    handles += [
        Line2D([], [], color='black', linewidth=1.0, alpha=0.7, label='median'),
        Line2D(
            [],
            [],
            color='black',
            linewidth=1.0,
            marker='_',
            markersize=13,
            markeredgewidth=1.2,
            alpha=0.7,
            label='min / max (clipped to 1–99 pct)',
        ),
    ]
    ax.grid(axis='y', alpha=0.3)

    ax = axes[1]
    n_methods = len(approaches)
    width = 0.8 / n_methods
    for i, a in enumerate(approaches):
        sub = big[big['approach'] == a]
        data = [
            _clip_to_percentile(
                sub[f'yoy_{y0}_{y1}'].dropna().to_numpy(dtype=float) * 100, 1, 99
            )
            for y0, y1 in YOY_TRANSITIONS
        ]
        pos = [
            j + 1 + (i - (n_methods - 1) / 2) * width
            for j in range(len(YOY_TRANSITIONS))
        ]
        good = [(p, d) for p, d in zip(pos, data) if d.size > 0]
        if not good:
            continue
        good_pos, good_data = zip(*good)
        parts = ax.violinplot(
            list(good_data),
            positions=list(good_pos),
            widths=width * 0.95,
            showmedians=True,
            showextrema=False,
        )
        _style_violin(parts, APPROACH_COLORS[a])
    ax.axhline(0, color='black', linewidth=0.8, alpha=0.6)
    ax.set_xticks([j + 1 for j in range(len(YOY_TRANSITIONS))])
    ax.set_xticklabels([f'{y0}→{y1}' for y0, y1 in YOY_TRANSITIONS])
    ax.set_title('YoY per transition, by method', fontsize=13)
    ax.set_ylabel('YoY (signed)')
    ax.set_ylim(*violin_ylim)
    ax.set_yticks(ticks)
    ax.set_yticklabels(tick_labels)
    ax.grid(axis='y', alpha=0.3)

    fig.suptitle(
        'Signed year-over-year change in N across A-matrix methods', fontsize=15
    )
    fig.legend(
        handles=handles,
        loc='lower center',
        ncol=len(handles),
        fontsize=10,
        frameon=False,
        bbox_to_anchor=(0.5, -0.04),
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches='tight')
    plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-dir', type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument('--skip-violin', action='store_true')
    args = parser.parse_args()
    main(results_dir=args.results_dir, skip_violin=args.skip_violin)
