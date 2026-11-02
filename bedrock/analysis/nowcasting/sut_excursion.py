"""How far each year of a nowcast build steps out of line, table by table.

The nowcast builds one balanced supply-use pair per year. A year whose structure
steps out and back, rather than lying on the path between its neighbours, shows
the method reacting to something in that year alone. This module measures that
**excursion** for every interior year and every block of the tables, measures
BEA's published summary tables the same way, and scores each build on disk
against BEA, so a change to the method can be judged by whether it brings the
tables closer to BEA's own year-to-year behaviour.

An excursion BEA's tables share is not a defect: the COVID shock (2020) and
rebound (2021) step out and back in the published tables too. So the score is
**ours minus published**, and :data:`EXPLAINED_YEARS` marks the years whose
shock is known; the build summary reports the mean with and without them.

Measure
-------

For one entity (an industry's input column, a commodity's buyer row, a
commodity's producer mix, ...), the structural dissimilarity between two years is
half the summed absolute change in its shares: 0 means the same structure, 1
means none in common. With ``d(a, b)`` that distance,

    excursion = d(t - 1, t) + d(t, t + 1) - d(t - 1, t + 1)

is the extra distance travelled by going through year t rather than straight
from t - 1 to t + 1. It is zero when t lies on the path between its neighbours
and at most ``2 * min(d(t - 1, t), d(t, t + 1))``. The level counterpart is
``log x(t) - mean(log x(t - 1), log x(t + 1))``.

Each block reports the size-weighted mean excursion and the share of the total
movement over the three years (``d(t - 1, t) + d(t, t + 1)``) it accounts for.

Builds are compared with BEA at **summary** level, ours summed to BEA's summary
rows and columns, since that is the grain BEA publishes every year.

Blocks
------

=================  ==============================  ==========================
block              entity, shares over             weight
=================  ==============================  ==========================
``use_columns``    industry, its commodity inputs  intermediate inputs
``use_rows``       commodity, its buyers + FD      commodity use
``supply_mix``     commodity, its producers        domestic output
``fd_columns``     final-demand column, commodity  column total
``va_columns``     industry, its value-added rows  value added
=================  ==============================  ==========================

CLI::

    uv run python -m bedrock.analysis.nowcasting.sut_excursion --check
    # one centre year of one build in depth, with its top detail contributors
    uv run python -m bedrock.analysis.nowcasting.sut_excursion \\
        --vintage v1.0.0_<hash> --year 2022
    # every year, every build on disk, against BEA
    uv run python -m bedrock.analysis.nowcasting.sut_excursion --span
"""

from __future__ import annotations

import argparse
import functools
import os
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_SUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: Interior years of the span: each needs the year before and after.
CENTRE_YEARS = tuple(range(2018, 2024))
#: Centre years whose excursion has a known cause shared with BEA's tables.
EXPLAINED_YEARS = {2020: 'COVID shock', 2021: 'COVID rebound'}
BLOCKS = ('use_columns', 'use_rows', 'supply_mix', 'fd_columns', 'va_columns')
#: Value-added rows of the supply-use Use table.
VA_ROWS = ('V00100', 'T00OTOP', 'T00OSUB', 'V00300', 'T00TOP', 'T00SUB')
#: The summary workbook's short value-added codes, as the detail tables write them.
PUBLISHED_ROW_CODES = {'V001': 'V00100', 'V003': 'V00300'}
#: Imports are a negative column of the Use; they are scored in the Supply.
IMPORTS = 'F05000'


class Tables(ta.NamedTuple):
    """One year's Use (commodity + VA rows x industry + FD columns) and Supply."""

    use: pd.DataFrame
    supply: pd.DataFrame


# --------------------------------------------------------------------------
# the measure
# --------------------------------------------------------------------------


def dissimilarity(a: pd.DataFrame, b: pd.DataFrame) -> pd.Series:
    """Half the summed absolute change in column shares, per column."""
    a, b = a.align(b, fill_value=0.0)
    share_a = a / a.sum().replace(0.0, np.nan)
    share_b = b / b.sum().replace(0.0, np.nan)
    return 0.5 * (share_a - share_b).abs().sum(min_count=1)


def excursion(frames: ta.Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Per column: d12, d23, d13, excursion and weight, from three years."""
    f1, f2, f3 = frames
    d12, d23, d13 = dissimilarity(f1, f2), dissimilarity(f2, f3), dissimilarity(f1, f3)
    weight = (f1.sum() + f2.sum() + f3.sum()) / 3
    out = pd.DataFrame(
        {'d12': d12, 'd23': d23, 'd13': d13, 'weight': weight.reindex(d12.index)}
    )
    out['excursion'] = (out['d12'] + out['d23'] - out['d13']).clip(lower=0.0)
    return out.dropna(subset=['d12', 'd23', 'd13'])


def level_excursion(totals: ta.Sequence[pd.Series]) -> pd.Series:
    """log x(2022) - mean(log x(2021), log x(2023)), positive totals only."""
    t1, t2, t3 = (t.where(t > 0) for t in totals)
    return (np.log(t2) - (np.log(t1) + np.log(t3)) / 2).dropna()


def summarise(table: pd.DataFrame) -> dict[str, float]:
    w = table['weight'].clip(lower=0.0)
    moved = (table['d12'] + table['d23']) * w
    return {
        'excursion': float((table['excursion'] * w).sum() / w.sum()),
        'share_of_movement': float((table['excursion'] * w).sum() / moved.sum()),
    }


# --------------------------------------------------------------------------
# blocks
# --------------------------------------------------------------------------


def blocks(tables: Tables, *, summary: bool) -> dict[str, pd.DataFrame]:
    """The five blocks of one year, entities as columns, non-negative values."""
    commodities, industries, fd = _axes(summary)
    use, supply = tables.use, tables.supply
    interior = use.reindex(index=commodities, columns=industries).fillna(0.0)
    final = use.reindex(index=commodities, columns=fd).fillna(0.0)
    va = use.reindex(index=list(VA_ROWS), columns=industries).fillna(0.0)
    make = supply.reindex(index=commodities, columns=industries).fillna(0.0)
    return {
        'use_columns': interior.clip(lower=0.0),
        'use_rows': pd.concat([interior, final], axis=1).clip(lower=0.0).T,
        'supply_mix': make.clip(lower=0.0).T,
        'fd_columns': final.clip(lower=0.0),
        # absolute values: taxes less subsidies and surplus can be negative
        'va_columns': va.abs(),
    }


def _first(mapping: ta.Mapping[ta.Any, ta.Sequence[str]]) -> dict[str, str]:
    return {k: v[0] for k, v in mapping.items() if v}


def _axes(summary: bool) -> tuple[list[str], list[str], list[str]]:
    fd = [c for c in USA_2017_FINAL_DEMAND_CODES if c != IMPORTS]
    if not summary:
        return list(USA_2017_COMMODITY_CODES), list(USA_2017_INDUSTRY_CODES), fd
    com = sorted(set(_first(load_bea_v2017_commodity_to_bea_v2017_summary()).values()))
    ind = sorted(set(_first(load_bea_v2017_industry_to_bea_v2017_summary()).values()))
    return com, ind, fd


def to_summary(tables: Tables) -> Tables:
    """Sum a detail year to BEA summary rows and columns."""
    com = _first(load_bea_v2017_commodity_to_bea_v2017_summary())
    ind = _first(load_bea_v2017_industry_to_bea_v2017_summary())

    def relabel(code: str, mapping: dict[str, str]) -> str:
        return mapping.get(code, code)

    use = tables.use.groupby(lambda r: relabel(r, com)).sum()
    use = use.T.groupby(lambda c: relabel(c, ind)).sum().T
    supply = tables.supply.groupby(lambda r: relabel(r, com)).sum()
    supply = supply.T.groupby(lambda c: relabel(c, ind)).sum().T
    return Tables(use, supply)


# --------------------------------------------------------------------------
# sources
# --------------------------------------------------------------------------


@functools.cache
def build_year(vintage: str, year: int) -> Tables:
    """A build's balanced supply-use pair, $M."""

    def read(name: str) -> pd.DataFrame:
        path = os.path.join(
            str(FBS_DIR), f'Balanced_Detail_{name}_{year}_{vintage}.parquet'
        )
        return pd.read_parquet(path)

    return Tables(read('Use_SUT'), read('Supply'))


@functools.cache
def published_year(year: int) -> Tables:
    """BEA's published summary supply-use pair, $M."""
    y = ta.cast(USA_SUMMARY_SUT_YEARS, year)

    def numeric(frame: pd.DataFrame) -> pd.DataFrame:
        frame = frame.apply(pd.to_numeric, errors='coerce').fillna(0.0)
        frame.index = (
            frame.index.astype(str)
            .str.strip()
            .map(lambda r: PUBLISHED_ROW_CODES.get(r, r))
        )
        # the summary workbook writes final-demand codes in four characters
        frame.columns = [
            f'{c}00' if len(c) == 4 and c.startswith('F') else c
            for c in frame.columns.astype(str).str.strip()
        ]
        return frame

    return Tables(
        numeric(_load_usa_summary_sut('Use_SUT_summary', y)),
        numeric(_load_usa_summary_sut('Supply_summary', y)),
    )


# --------------------------------------------------------------------------
# assessment
# --------------------------------------------------------------------------


def assess(years: ta.Sequence[Tables], *, summary: bool) -> pd.DataFrame:
    """Weighted excursion and its share of movement, per block."""
    per_year = [blocks(t, summary=summary) for t in years]
    rows = {}
    for block in BLOCKS:
        rows[block] = summarise(excursion([y[block] for y in per_year]))
    industry_output = [t.supply.reindex(columns=_axes(summary)[1]).sum() for t in years]
    commodity_output = [
        t.supply.reindex(index=_axes(summary)[0]).sum(axis=1) for t in years
    ]
    for name, totals in (('industry_output', industry_output),
                         ('commodity_output', commodity_output)):  # fmt: skip
        level = level_excursion(totals).abs()
        weight = totals[1].reindex(level.index).clip(lower=0.0)
        rows[f'{name}_level'] = {
            'excursion': float((level * weight).sum() / weight.sum()),
            'share_of_movement': np.nan,
        }
    return pd.DataFrame(rows).T


def top_entities(years: ta.Sequence[Tables], block: str, n: int = 10) -> pd.DataFrame:
    """The detail entities contributing most weighted excursion to *block*."""
    table = excursion([blocks(t, summary=False)[block] for t in years])
    table['contribution'] = table['excursion'] * table['weight']
    table['contribution'] /= table['contribution'].sum()
    return table.sort_values('contribution', ascending=False).head(n)


def report(vintage: str, year: int) -> None:
    """One centre year of one build: every block, against BEA, with contributors."""
    pd.set_option('display.width', 220)
    years = (year - 1, year, year + 1)
    ours = [build_year(vintage, y) for y in years]
    columns = {
        'published (summary)': assess([published_year(y) for y in years], summary=True),
        'ours (summary)': assess([to_summary(t) for t in ours], summary=True),
        'ours (detail)': assess(ours, summary=False),
    }
    print(f'== {year} excursion, {vintage}')
    print('\nWeighted mean excursion (0-1 for structure, |log| for levels):')
    print(
        pd.DataFrame({k: v['excursion'] for k, v in columns.items()})
        .round(4)
        .to_string()
    )
    print(f'\nExcursion as a share of {years[0]}-{years[2]} movement:')
    print(
        pd.DataFrame({k: v['share_of_movement'] for k, v in columns.items()})
        .drop(index=['industry_output_level', 'commodity_output_level'])
        .map('{:.0%}'.format)
        .to_string()
    )
    for block in BLOCKS:
        print(f'\nTop detail contributors, {block}:')
        print(top_entities(ours, block).round(3).to_string())


# --------------------------------------------------------------------------
# the span, and build vintages
# --------------------------------------------------------------------------


def build_vintages() -> list[str]:
    """Builds with balanced tables on disk, oldest first by creation date.

    The date is the metadata sidecar's ``date_created``; a build downloaded
    without its sidecar falls back to the parquet's modification time.
    """
    import datetime  # noqa: PLC0415
    import glob  # noqa: PLC0415
    import json  # noqa: PLC0415

    created: dict[str, str] = {}
    for path in glob.glob(
        os.path.join(str(FBS_DIR), 'Balanced_Detail_Use_SUT_*.parquet')
    ):
        name = os.path.basename(path)[len('Balanced_Detail_Use_SUT_') :]
        vintage = name[5:].replace('.parquet', '')
        meta = path.replace('.parquet', '_metadata.json')
        if os.path.exists(meta):
            with open(meta, encoding='utf-8') as f:
                date = str(json.load(f).get('date_created', ''))
        else:
            date = datetime.datetime.fromtimestamp(os.path.getmtime(path)).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
        created[vintage] = min(created.get(vintage, date), date)
    return sorted(created, key=lambda v: created[v])


def _has_year(vintage: str, year: int) -> bool:
    return os.path.exists(
        os.path.join(str(FBS_DIR), f'Balanced_Detail_Use_SUT_{year}_{vintage}.parquet')
    )


def span_scores(source: str) -> pd.DataFrame:
    """Summary-level excursion, centre year x block, for a vintage or BEA.

    *source* is a build vintage, or ``'published'``. Centre years whose
    neighbours are not built are left out.
    """
    rows = {}
    for centre in CENTRE_YEARS:
        years = (centre - 1, centre, centre + 1)
        if source == 'published':
            tables = [published_year(y) for y in years]
        elif all(_has_year(source, y) for y in years):
            tables = [to_summary(build_year(source, y)) for y in years]
        else:
            continue
        rows[centre] = assess(tables, summary=True)['excursion']
    return pd.DataFrame(rows).T


def span_report(vintages: ta.Sequence[str]) -> pd.DataFrame:
    """Each build's excess excursion over BEA's, by centre year and block."""
    pd.set_option('display.width', 220)
    published = span_scores('published')
    print('== BEA published summary tables: excursion by centre year')
    print(published.round(4).to_string())
    scores = {}
    for vintage in vintages:
        ours = span_scores(vintage)
        if ours.empty:
            continue
        excess = ours - published.reindex(ours.index)
        print(f'\n== {vintage}: ours minus published')
        print(excess.round(4).to_string())
        unexplained = excess.drop(index=list(EXPLAINED_YEARS), errors='ignore')
        scores[vintage] = {
            'years': ','.join(str(y) for y in ours.index),
            'mean excess, all years': float(excess.to_numpy().mean()),
            'mean excess, unexplained years': float(unexplained.to_numpy().mean()),
            'worst year': int(excess.mean(axis=1).idxmax()),
            'worst year excess': float(excess.mean(axis=1).max()),
        }
    summary = pd.DataFrame(scores).T
    print('\n== Builds, oldest first (lower excess is closer to BEA)')
    print(f'explained years: {EXPLAINED_YEARS}')
    print(summary.to_string())
    return summary


def check() -> None:
    a = pd.DataFrame({'x': [1.0, 0.0], 'y': [1.0, 1.0]})
    b = pd.DataFrame({'x': [0.0, 1.0], 'y': [1.0, 1.0]})
    d = dissimilarity(a, b)
    assert d['x'] == 1.0 and d['y'] == 0.0
    # a straight path has no excursion; out and back has all of it
    mid = pd.DataFrame({'x': [0.5, 0.5], 'y': [1.0, 1.0]})
    assert np.isclose(excursion([a, mid, b])['excursion']['x'], 0.0)
    out_and_back = excursion([a, b, a])
    assert np.isclose(float(out_and_back['excursion']['x']), 2.0)
    level = level_excursion([pd.Series([1.0]), pd.Series([np.e]), pd.Series([1.0])])
    assert np.isclose(level.iloc[0], 1.0)
    print('check passed')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--vintage', help='the build, v<version>_<hash>')
    parser.add_argument(
        '--year', type=int, default=2022, choices=CENTRE_YEARS,
        help='centre year for the one-build report',
    )  # fmt: skip
    parser.add_argument(
        '--span', action='store_true', help='every year, every build on disk'
    )
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        check()
        return
    if args.span:
        span_report([args.vintage] if args.vintage else build_vintages())
        return
    if not args.vintage:
        parser.error('--vintage is required')
    report(args.vintage, args.year)


if __name__ == '__main__':
    main()
