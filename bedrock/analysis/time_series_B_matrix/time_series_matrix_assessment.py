"""FBS sector E and real commodity B over the time series assessment.

Writes row SVGs (E Mt / E normalized; B intensity / B normalized), absolute
line SVGs for the top codes by range, and one CSV joined on BEA detail
``code``. Absolute top-N order is reused for matching normalized row plots.
Same code is the primary industry/commodity pair; B reshuffles via
``(E/x)@Vnorm``, so values need not match.

B uses ``B_total(..., real=True)`` after rebasing dig ``x`` to constant
**2017$**. E is industry CO2e at GHG FBS ``v0.6.0_635777c`` (dig
``E_stratified`` cache when present). Normalization defaults to 2017 = 1.

Usage::

    uv run python -m bedrock.analysis.time_series_B_matrix.time_series_matrix_assessment

    uv run python -m bedrock.analysis.time_series_B_matrix.time_series_matrix_assessment \\
        --codes 221100,324110,211000
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    CACHE_BEARING_MODULES,
    CACHE_DIR,
    CONFIG_TEMPLATE,
    OUTPUT_DIR,
    YEARS,
    B_total,
    deflate_x,
    fbs_to_co2e,
    load_fbs,
    load_span,
)
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

PlotScale = Literal['normalized', 'Mt', 'intensity']
AbsKind = Literal['Mt', 'intensity']

#: Constant-dollar year for real B (and default normalization year).
DOLLAR_YEAR = 2017

#: Distinct hues on one warm→cool path (2017 gold … 2024 navy).
YEAR_COLORS: dict[int, str] = {
    2017: '#FFD400',
    2018: '#FF8C00',
    2019: '#E41A1C',
    2020: '#C51B8A',
    2021: '#7A0177',
    2022: '#54278F',
    2023: '#2171B5',
    2024: '#08306B',
}


def sector_emissions_panel(
    years: tuple[int, ...] = YEARS,
    codes: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Sector x year CO2e totals from the year-stem GHG FBS (kg).

    Index = sector code; columns = years. FBS pin is ``v0.6.0_635777c``.
    """
    cache_e = CACHE_DIR / 'E_stratified.parquet'
    if cache_e.exists():
        logger.info(f'Loading sector E from {cache_e}')
        E = pd.read_parquet(cache_e)
        E = E[E['year'].isin(years)]
        panel = (
            E.groupby(['sector', 'year'], observed=True)['CO2e']
            .sum()
            .unstack('year')
            .fillna(0.0)
        )
    else:
        logger.info('Building sector E from FBS parquets at v0.6.0_635777c')
        parts: dict[int, pd.Series] = {}
        for year in years:
            with temp_usa_config(
                CONFIG_TEMPLATE.format(year=year),
                cache_bearing_modules=CACHE_BEARING_MODULES,
            ):
                co2e = fbs_to_co2e(load_fbs(year, 'v0.6.0_635777c'))
            parts[year] = (
                co2e.groupby(co2e['SectorProducedBy'].astype(str), observed=True)[
                    'CO2e'
                ]
                .sum()
                .rename(year)
            )
        panel = pd.DataFrame(parts).fillna(0.0)
        panel.index.name = 'sector'

    panel.index = panel.index.map(str)
    panel.columns = pd.Index([int(c) for c in panel.columns], name='year')
    if codes is not None:
        wanted = [str(s) for s in codes]
        missing = [s for s in wanted if s not in panel.index]
        if missing:
            raise KeyError(f'Codes not in FBS E panel: {missing}')
        panel = panel.reindex(wanted)
    return panel.sort_index(axis='columns')


def commodity_B_panel(
    years: tuple[int, ...] = YEARS,
    codes: Sequence[str] | None = None,
    dollar_year: int = DOLLAR_YEAR,
) -> pd.DataFrame:
    """Commodity x year real B (kg CO2e per *dollar_year*$), from dig ``load_span``.

    Rebases nominal ``span.x`` with ``deflate_x(..., base_year=dollar_year)`` then
    ``B_total(span, real=True)``. Dig cache ``x_real`` is ignored for the dollar base.
    """
    logger.info(f'Loading dig span; rebasing x to {dollar_year}$ for real B_total')
    span = load_span(years)
    with temp_usa_config(
        CONFIG_TEMPLATE.format(year=dollar_year),
        cache_bearing_modules=CACHE_BEARING_MODULES,
        nowcast_mut_vintage=span.vintages.mut,
    ):
        span.x_real = deflate_x(span.x, base_year=dollar_year)
    panel = B_total(span, real=True)
    panel = panel.reindex(columns=list(years))
    panel.index = panel.index.map(str)
    panel.columns = pd.Index([int(c) for c in panel.columns], name='year')
    if codes is not None:
        wanted = [str(s) for s in codes]
        missing = [s for s in wanted if s not in panel.index]
        if missing:
            raise KeyError(f'Codes not in real B panel: {missing}')
        panel = panel.reindex(wanted)
    return panel.sort_index(axis='columns')


def absolute_top_order(
    panel: pd.DataFrame,
    *,
    abs_kind: AbsKind,
    base_year: int = DOLLAR_YEAR,
    top_n: int | None = 50,
) -> list[str]:
    """Codes ranked by absolute max-min range (largest first), optionally truncated.

    Drops rows with non-positive *base_year* so the same order can normalize.
    *abs_kind* ``Mt`` treats *panel* as kg CO2e; ``intensity`` uses values as-is.
    """
    if base_year not in panel.columns:
        raise KeyError(
            f'base_year {base_year} not in panel columns {list(panel.columns)}'
        )
    absolute = (panel / 1e9) if abs_kind == 'Mt' else panel.copy()
    absolute = absolute.loc[absolute[base_year] > 0]
    swing = absolute.max(axis=1) - absolute.min(axis=1)
    order = [str(c) for c in swing.sort_values(ascending=False).index.tolist()]
    if top_n is not None:
        order = order[: int(top_n)]
    return order


def plot_by_year(
    panel: pd.DataFrame,
    *,
    name_by_code: Mapping[str, str],
    y_scale: PlotScale,
    title: str,
    out_stem: str,
    base_year: int = DOLLAR_YEAR,
    top_n: int | None = 50,
    order: Sequence[str] | None = None,
    abs_unit: str = '',
    dollar_year: int = DOLLAR_YEAR,
) -> Path:
    """Year-panel plot: rows = codes, dots = years. SVG only (no per-plot CSV).

    *y_scale* ``normalized`` divides by *base_year* (must be positive).
    ``Mt`` divides kg by 1e9. ``intensity`` plots panel values as-is.
    Normalized y-labels append the base-year absolute when *abs_unit* is set.
    *order* fixes row codes and sequence (no re-rank). If omitted, rank by this
    plot's max-min range then apply *top_n* (None = all).
    """
    years = [int(y) for y in panel.columns]
    if not years:
        raise ValueError('panel has no year columns')

    intensity_label = f'kg CO2e / ${dollar_year}'
    if y_scale == 'Mt':
        absolute = panel / 1e9
        values = absolute.copy()
        xlabel = 'Mt CO2e'
        ref = None
        scale_note = 'Mt CO2e'
    elif y_scale == 'intensity':
        absolute = panel.copy()
        values = absolute.copy()
        xlabel = intensity_label
        ref = None
        scale_note = intensity_label
    else:
        if base_year not in panel.columns:
            raise KeyError(f'base_year {base_year} not in panel columns {years}')
        absolute = (panel / 1e9) if abs_unit == 'Mt' else panel.copy()
        base = absolute[base_year]
        positive = base > 0
        absolute = absolute.loc[positive]
        values = absolute.div(base[positive], axis=0)
        xlabel = f'normalized ({base_year} = 1)'
        ref = 1.0
        scale_note = f'{base_year}=1 normalized'

    if order is not None:
        plot_order = [str(c) for c in order]
        missing = [c for c in plot_order if c not in values.index]
        if missing:
            raise KeyError(f'order codes not in plot panel: {missing}')
    else:
        swing = values.max(axis=1) - values.min(axis=1)
        plot_order = [str(c) for c in swing.sort_values(ascending=False).index.tolist()]
        if top_n is not None:
            plot_order = plot_order[: int(top_n)]

    values_plot = values.reindex(plot_order)
    absolute_plot = absolute.reindex(plot_order)
    n = len(values_plot)
    fig_h = max(4.5, 0.28 * n + 1.8)
    fig, ax = plt.subplots(figsize=(11, fig_h))
    for i, code in enumerate(values_plot.index):
        y = n - 1 - i
        row = values_plot.loc[code]
        lo, hi = float(row.min()), float(row.max())
        ax.hlines(y, lo, hi, color='0.75', linewidth=1.4, zorder=1)
        for year in years:
            color = YEAR_COLORS.get(year, '#333333')
            ax.scatter(
                float(row[year]),
                y,
                s=55,
                color=color,
                zorder=2,
                edgecolors='0.15',
                linewidths=0.45,
            )

    if ref is not None:
        ax.axvline(ref, color='0.35', linestyle='--', linewidth=1)
        ax.axvline(2.0, color='red', linestyle=':', linewidth=1.2)
        ax.axvline(0.5, color='red', linestyle=':', linewidth=1.2)

    tick_labels: list[str] = []
    for code in reversed(values_plot.index):
        name = name_by_code.get(str(code), '')
        label = f'{code} {name}'.strip()
        if y_scale == 'normalized' and abs_unit:
            base_abs = float(absolute_plot.loc[code, base_year])
            if abs_unit == 'Mt':
                label = f'{label} ({base_abs:.3g} Mt)'
            else:
                label = f'{label} ({base_abs:.3g} {abs_unit})'
        tick_labels.append(label)

    ax.set_yticks(list(range(n)))
    ax.set_yticklabels(tick_labels, fontsize=8)
    ax.set_xlabel(xlabel)
    ax.set_ylim(-0.6, n - 0.4)
    ax.set_title(f'{title} ({scale_note})')
    handles = [
        plt.Line2D(
            [0],
            [0],
            marker='o',
            color='w',
            markerfacecolor=YEAR_COLORS.get(y, '#333333'),
            markeredgecolor='0.15',
            markersize=8,
            label=str(y),
        )
        for y in years
    ]
    if ref is not None:
        handles.append(
            plt.Line2D([0], [0], color='0.35', linestyle='--', label=f'{base_year} = 1')
        )
        handles.append(
            plt.Line2D([0], [0], color='red', linestyle=':', label='2× / 0.5×')
        )
    ax.legend(handles=handles, title='year', loc='lower right', fontsize=7, ncol=2)
    fig.tight_layout()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / f'{out_stem}.svg'
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    logger.info(f'Wrote {out}')
    return out


def plot_lines_by_year(
    panel: pd.DataFrame,
    *,
    name_by_code: Mapping[str, str],
    abs_kind: AbsKind,
    title: str,
    out_stem: str,
    order: Sequence[str],
    dollar_year: int = DOLLAR_YEAR,
) -> Path:
    """Line chart of absolute levels over years for the codes in *order*.

    *abs_kind* ``Mt`` treats *panel* as kg CO2e; ``intensity`` uses values as-is
    (kg per *dollar_year*$). No normalization.
    """
    years = [int(y) for y in panel.columns]
    if not years:
        raise ValueError('panel has no year columns')
    absolute = (panel / 1e9) if abs_kind == 'Mt' else panel.copy()
    codes = [str(c) for c in order]
    missing = [c for c in codes if c not in absolute.index]
    if missing:
        raise KeyError(f'order codes not in panel: {missing}')
    absolute = absolute.reindex(codes)

    ylabel = 'Mt CO2e' if abs_kind == 'Mt' else f'kg CO2e / ${dollar_year}'
    cmap = plt.get_cmap('tab20')
    fig, ax = plt.subplots(figsize=(12, 7))
    for i, code in enumerate(codes):
        row = absolute.loc[code]
        color = cmap(i % 20)
        name = name_by_code.get(code, '')
        label = f'{code} {name}'.strip()
        if len(label) > 42:
            label = f'{label[:39]}...'
        ax.plot(
            years,
            [float(row[y]) for y in years],
            marker='o',
            markersize=4,
            linewidth=1.5,
            color=color,
            label=label,
        )
    ax.set_xlabel('year')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(years)
    ax.legend(loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=7)
    fig.tight_layout()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / f'{out_stem}.svg'
    fig.savefig(out, bbox_inches='tight')
    plt.close(fig)
    logger.info(f'Wrote {out}')
    return out


def joined_E_B_csv(
    E_kg: pd.DataFrame,
    B_intensity: pd.DataFrame,
    *,
    base_year: int = DOLLAR_YEAR,
    path: Path | None = None,
) -> Path:
    """Outer-join E (Mt + normalized) and real B (intensity + normalized) on code."""
    years = sorted(
        {int(c) for c in E_kg.columns} | {int(c) for c in B_intensity.columns}
    )
    industry_name = {str(k): str(v) for k, v in INDUSTRY_DESC.items()}
    commodity_name = {str(k): str(v) for k, v in COMMODITY_DESC.items()}

    E_mt = E_kg / 1e9
    E_base = E_mt[base_year] if base_year in E_mt.columns else pd.Series(dtype=float)
    E_norm = E_mt.div(E_base.where(E_base > 0), axis=0)
    E_disc_mt = E_mt.max(axis=1) - E_mt.min(axis=1)
    E_disc_norm = E_norm.max(axis=1) - E_norm.min(axis=1)

    B = B_intensity
    B_base = B[base_year] if base_year in B.columns else pd.Series(dtype=float)
    B_norm = B.div(B_base.where(B_base > 0), axis=0)
    B_disc_int = B.max(axis=1) - B.min(axis=1)
    B_disc_norm = B_norm.max(axis=1) - B_norm.min(axis=1)

    codes = sorted(set(E_mt.index.astype(str)) | set(B.index.astype(str)))
    rows: list[dict[str, object]] = []
    for code in codes:
        row: dict[str, object] = {
            'code': code,
            'industry_label': industry_name.get(code, ''),
            'commodity_label': commodity_name.get(code, ''),
            'E_range_Mt': (
                float(E_disc_mt[code]) if code in E_disc_mt.index else float('nan')
            ),
            'E_range_normalized': (
                float(E_disc_norm[code]) if code in E_disc_norm.index else float('nan')
            ),
            'B_range_intensity': (
                float(B_disc_int[code]) if code in B_disc_int.index else float('nan')
            ),
            'B_range_normalized': (
                float(B_disc_norm[code]) if code in B_disc_norm.index else float('nan')
            ),
        }
        for year in years:
            row[f'E_{year}_Mt'] = (
                float(E_mt.loc[code, year])
                if code in E_mt.index and year in E_mt.columns
                else float('nan')
            )
            row[f'E_{year}_normalized'] = (
                float(E_norm.loc[code, year])
                if code in E_norm.index and year in E_norm.columns
                else float('nan')
            )
            row[f'B_{year}_intensity'] = (
                float(B.loc[code, year])
                if code in B.index and year in B.columns
                else float('nan')
            )
            row[f'B_{year}_normalized'] = (
                float(B_norm.loc[code, year])
                if code in B_norm.index and year in B_norm.columns
                else float('nan')
            )
        rows.append(row)

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(
        by=['E_range_normalized', 'B_range_normalized'],
        ascending=False,
        na_position='last',
    )
    col_order = (
        ['code', 'industry_label', 'commodity_label', 'E_range_Mt']
        + [f'E_{year}_Mt' for year in years]
        + ['E_range_normalized']
        + [f'E_{year}_normalized' for year in years]
        + ['B_range_intensity']
        + [f'B_{year}_intensity' for year in years]
        + ['B_range_normalized']
        + [f'B_{year}_normalized' for year in years]
    )
    out_df = out_df.reindex(columns=col_order)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = path or (OUTPUT_DIR / 'fbs_E_B_by_year.csv')
    out_df.to_csv(csv_path, index=False)
    logger.info(f'Wrote {csv_path}')
    return csv_path


def main(
    years: tuple[int, ...] = YEARS,
    codes: Sequence[str] | None = None,
    base_year: int = DOLLAR_YEAR,
    top_n: int | None = 50,
    line_top_n: int | None = 20,
    dollar_year: int = DOLLAR_YEAR,
) -> list[Path]:
    """Write row + line SVGs and one joined E+B CSV under ``output/``."""
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    E_kg = sector_emissions_panel(years, codes=codes)
    B = commodity_B_panel(years, codes=codes, dollar_year=dollar_year)

    industry_name = {str(k): str(v) for k, v in INDUSTRY_DESC.items()}
    commodity_name = {str(k): str(v) for k, v in COMMODITY_DESC.items()}

    e_order = absolute_top_order(E_kg, abs_kind='Mt', base_year=base_year, top_n=top_n)
    b_order = absolute_top_order(
        B, abs_kind='intensity', base_year=base_year, top_n=top_n
    )
    e_line_order = absolute_top_order(
        E_kg, abs_kind='Mt', base_year=base_year, top_n=line_top_n
    )
    b_line_order = absolute_top_order(
        B, abs_kind='intensity', base_year=base_year, top_n=line_top_n
    )
    b_abs_unit = f'kg/${dollar_year}'

    paths = [
        plot_by_year(
            E_kg,
            name_by_code=industry_name,
            y_scale='Mt',
            title='FBS sector E by year',
            out_stem='fbs_sector_E_by_year_Mt',
            base_year=base_year,
            order=e_order,
        ),
        plot_by_year(
            E_kg,
            name_by_code=industry_name,
            y_scale='normalized',
            title='FBS sector E by year',
            out_stem='fbs_sector_E_by_year_normalized',
            base_year=base_year,
            order=e_order,
            abs_unit='Mt',
        ),
        plot_by_year(
            B,
            name_by_code=commodity_name,
            y_scale='intensity',
            title=f'Commodity B by year (real {dollar_year}$)',
            out_stem='fbs_commodity_B_by_year_intensity',
            base_year=base_year,
            order=b_order,
            dollar_year=dollar_year,
        ),
        plot_by_year(
            B,
            name_by_code=commodity_name,
            y_scale='normalized',
            title=f'Commodity B by year (real {dollar_year}$)',
            out_stem='fbs_commodity_B_by_year_normalized',
            base_year=base_year,
            order=b_order,
            abs_unit=b_abs_unit,
            dollar_year=dollar_year,
        ),
        plot_lines_by_year(
            E_kg,
            name_by_code=industry_name,
            abs_kind='Mt',
            title='FBS sector E by year',
            out_stem='fbs_sector_E_by_year_Mt_lines',
            order=e_line_order,
            dollar_year=dollar_year,
        ),
        plot_lines_by_year(
            B,
            name_by_code=commodity_name,
            abs_kind='intensity',
            title=f'Commodity B by year (real {dollar_year}$)',
            out_stem='fbs_commodity_B_by_year_intensity_lines',
            order=b_line_order,
            dollar_year=dollar_year,
        ),
        joined_E_B_csv(E_kg, B, base_year=base_year),
    ]
    return paths


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument(
        '--years',
        nargs='+',
        type=int,
        default=list(YEARS),
        help='Calendar years in the panel (default: 2017-2024)',
    )
    parser.add_argument(
        '--codes',
        default=None,
        help='Comma-separated BEA detail codes (default: all)',
    )
    parser.add_argument(
        '--base-year',
        type=int,
        default=DOLLAR_YEAR,
        help=f'Normalization year = 1 (default: {DOLLAR_YEAR})',
    )
    parser.add_argument(
        '--dollar-year',
        type=int,
        default=DOLLAR_YEAR,
        help=f'Constant-dollar year for real B (default: {DOLLAR_YEAR})',
    )
    parser.add_argument(
        '--top-n',
        type=int,
        default=50,
        help='Row SVG only: top N by absolute max-min range (0 = all)',
    )
    parser.add_argument(
        '--line-top-n',
        type=int,
        default=20,
        help='Line SVG only: top N by absolute max-min range (0 = all)',
    )
    args = parser.parse_args()
    code_list = (
        None
        if not args.codes
        else [s.strip() for s in str(args.codes).split(',') if s.strip()]
    )
    top = None if int(args.top_n) <= 0 else int(args.top_n)
    line_top = None if int(args.line_top_n) <= 0 else int(args.line_top_n)
    for path in main(
        years=tuple(args.years),
        codes=code_list,
        base_year=args.base_year,
        top_n=top,
        line_top_n=line_top,
        dollar_year=args.dollar_year,
    ):
        print(path)
