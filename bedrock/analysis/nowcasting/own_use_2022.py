"""Own-use cells and the Make producer mix around 2022 (#890).

"Own use" is an industry's purchases of its own commodity: the diagonal of the
Use table. #890 found that our own-use cells and the Make producer mix both
bump in 2022 more than BEA's published summary tables do. This module measures
that bump on any build's balanced supply-use tables (``Balanced_Detail_Use_SUT``
and ``Balanced_Detail_Supply``, before redefinitions, under ``FBS_DIR``), so one
build can be compared with another (for example before and after the #1157
vintage fix). Those parquets are local outputs of the nowcast build; the stored
MUT on GCS is after redefinitions, which moves own use, so it is not used.

Measures
--------

**Bump ratio.** ``value(2022) / mean(value(2021), value(2023))``. 1.0 means
2022 sits on the line between its neighbours. The published summary Use bumps
too, because of the 2022 commodity price spike, so the defect is our ratio
*above* the published one, not the bump itself.

**Summary own use.** The balanced detail Use is summed to BEA summary rows and
columns, and its summary diagonal is compared with the published summary Use
(supply-use framework) that the build reads, ``io_2017.SUMMARY_SUT_MAPPING``.
Both are the supply-use framework at purchasers' prices, before redefinitions.

**Make mix distance.** For each commodity, half the summed absolute change in
its producers' shares since 2017, weighted by the commodity's output.

CLI::

    uv run python -m bedrock.analysis.nowcasting.own_use_2022 --check
    uv run python -m bedrock.analysis.nowcasting.own_use_2022 \\
        --vintage v0.5.0_a166975 --compare <new vintage>
"""

from __future__ import annotations

import argparse
import functools
import os
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import SUMMARY_SUT_MAPPING, _load_usa_summary_sut
from bedrock.utils.config.settings import FBS_DIR
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_SUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

YEARS = tuple(range(2017, 2025))
BUMP_YEAR = 2022
DEFAULT_VINTAGE = 'v0.5.0_a166975'

#: Detail own-use cells counted in the peak-year table: 2017 own use above this.
DETAIL_FLOOR = 200e6
#: Summary own-use cells counted in the peak-year table: 2017 own use above this.
SUMMARY_FLOOR = 1e9

#: The summary industries #890 lists, and the cells it calls out.
WATCH_SUMMARY = (
    '42', '61', '484', '711AS', '621', '212', '322', '311FT', '511', '327',
    '722', '22', '211', '332',
)  # fmt: skip


def bump_ratio(series: pd.Series) -> float:
    """2022 over the mean of 2021 and 2023."""
    neighbours = (series[BUMP_YEAR - 1] + series[BUMP_YEAR + 1]) / 2
    return float(series[BUMP_YEAR] / neighbours) if neighbours else np.nan


#: The balanced supply-use parquets are in millions of dollars.
MILLIONS = 1e6


def _balanced(name: str, vintage: str, year: int) -> pd.DataFrame:
    """One balanced supply-use table, USD."""
    path = os.path.join(
        str(FBS_DIR), f'Balanced_Detail_{name}_{year}_{vintage}.parquet'
    )
    if not os.path.exists(path):
        raise FileNotFoundError(f'{path} not built; run the nowcast for {vintage}')
    return pd.read_parquet(path) * MILLIONS


@functools.cache
def _use(vintage: str, year: int) -> pd.DataFrame:
    """Intermediate block, commodity x industry, USD."""
    return (
        _balanced('Use_SUT', vintage, year)
        .reindex(
            index=list(USA_2017_COMMODITY_CODES),
            columns=list(USA_2017_INDUSTRY_CODES),
        )
        .fillna(0.0)
    )


@functools.cache
def _make(vintage: str, year: int) -> pd.DataFrame:
    """Domestic output, industry x commodity, USD: the Supply industry block."""
    supply = _balanced('Supply', vintage, year)
    return (
        supply.reindex(
            index=list(USA_2017_COMMODITY_CODES),
            columns=list(USA_2017_INDUSTRY_CODES),
        )
        .fillna(0.0)
        .T
    )


def _first(mapping: ta.Mapping[ta.Any, ta.Sequence[str]]) -> dict[str, str]:
    return {k: v[0] for k, v in mapping.items() if v}


def detail_own_use(vintage: str) -> pd.DataFrame:
    """Detail diagonal, code x year, USD."""
    codes = [c for c in USA_2017_INDUSTRY_CODES if c in set(USA_2017_COMMODITY_CODES)]
    return pd.DataFrame(
        {y: [_use(vintage, y).at[c, c] for c in codes] for y in YEARS}, index=codes
    )


def summary_use(vintage: str, year: int) -> pd.DataFrame:
    """Our detail Use summed to summary commodity rows x summary industry columns."""
    use = _use(vintage, year)
    rows = use.index.map(_first(load_bea_v2017_commodity_to_bea_v2017_summary()))
    cols = use.columns.map(_first(load_bea_v2017_industry_to_bea_v2017_summary()))
    return use.groupby(rows).sum().T.groupby(cols).sum().T


def summary_own_use(vintage: str) -> pd.DataFrame:
    frames = {}
    for y in YEARS:
        s = summary_use(vintage, y)
        codes = s.index.intersection(s.columns)
        frames[y] = pd.Series(np.diag(s.loc[codes, codes]), index=codes)
    return pd.DataFrame(frames)


@functools.cache
def published_summary_own_use() -> pd.DataFrame:
    """Published summary Use diagonal, USD, from the workbook the build reads."""
    frames = {}
    for y in YEARS:
        pub = _load_usa_summary_sut(
            'Use_SUT_summary', ta.cast(USA_SUMMARY_SUT_YEARS, y)
        )
        pub = pub.apply(pd.to_numeric, errors='coerce').fillna(0.0)
        codes = pub.index.intersection(pub.columns)
        frames[y] = pd.Series(np.diag(pub.loc[codes, codes]), index=codes) * 1e6
    return pd.DataFrame(frames)


def peak_year_counts(own: pd.DataFrame, floor: float) -> pd.Series:
    cells = own[own[YEARS[0]] > floor]
    return cells.idxmax(axis=1).value_counts().reindex(YEARS, fill_value=0)


def bump_table(vintage: str) -> pd.DataFrame:
    ours, pub = summary_own_use(vintage), published_summary_own_use()
    codes = [c for c in WATCH_SUMMARY if c in ours.index and c in pub.index]
    ours, pub = ours.loc[codes].astype(float), pub.loc[codes].astype(float)
    return pd.DataFrame(
        {
            'ours': ours.apply(bump_ratio, axis=1),
            'published': pub.apply(bump_ratio, axis=1),
            'ours_2022_bn': ours[BUMP_YEAR] / 1e9,
            'ours_over_pub_2017': ours[2017] / pub[2017],
            'ours_over_pub_2024': ours[2024] / pub[2024],
        }
    )


def make_mix_distance(vintage: str) -> pd.Series:
    """Output-weighted producer-mix distance from 2017, per year."""
    base = _make(vintage, 2017)
    base_share = base / base.sum().replace(0, np.nan)
    out = {}
    for y in YEARS:
        make = _make(vintage, y)
        q = make.sum()
        share = make / q.replace(0, np.nan)
        distance = 0.5 * (share - base_share).abs().sum()
        out[y] = float((distance * q).sum() / q[distance.notna()].sum())
    return pd.Series(out)


def make_mix_movers(vintage: str, top: int = 10) -> pd.DataFrame:
    """The (commodity, producer) shares that move most out and back at 2022."""
    shares = {}
    for y in (BUMP_YEAR - 1, BUMP_YEAR, BUMP_YEAR + 1):
        make = _make(vintage, y)
        shares[y] = (make / make.sum().replace(0, np.nan)).stack()
    df = pd.DataFrame(shares).fillna(0.0)
    df['excursion'] = df[BUMP_YEAR] - (df[BUMP_YEAR - 1] + df[BUMP_YEAR + 1]) / 2
    df.index.names = ['industry', 'commodity']
    return df.reindex(df['excursion'].abs().sort_values(ascending=False).index).head(
        top
    )


def report(vintage: str) -> None:
    pd.set_option('display.width', 200)
    print(f'== {vintage}; published = {SUMMARY_SUT_MAPPING["Use_SUT_summary"]}')
    detail = detail_own_use(vintage)
    ours, pub = summary_own_use(vintage), published_summary_own_use()
    print('\nPeak year of own use:')
    print(
        pd.DataFrame(
            {
                f'detail (2017 > ${DETAIL_FLOOR / 1e6:.0f}M)': peak_year_counts(
                    detail, DETAIL_FLOOR
                ),
                f'ours, summary (> ${SUMMARY_FLOOR / 1e9:.0f}bn)': peak_year_counts(
                    ours, SUMMARY_FLOOR
                ),
                'published, summary': peak_year_counts(pub, SUMMARY_FLOOR),
            }
        ).T.to_string()
    )
    big = ours.index[ours[2017] > SUMMARY_FLOOR].intersection(pub.index)
    print(
        f'\nMedian bump ratio, summary cells: ours '
        f'{ours.loc[big].apply(bump_ratio, axis=1).median():.3f}, published '
        f'{pub.loc[big].apply(bump_ratio, axis=1).median():.3f}'
    )
    print('\nWatched summary industries:')
    print(bump_table(vintage).round(2).to_string())
    print('\nTruck and utilities own use, $bn:')
    rows = {
        '484000 ours (detail)': detail.loc['484000'],
        '484 published': pub.loc['484'],
        '22 ours (summary)': ours.loc['22'],
        '22 published': pub.loc['22'],
        '221100 ours (detail)': detail.loc['221100'],
    }
    print((pd.DataFrame(rows).T / 1e9).round(1).to_string())
    print('\nMake mix distance from 2017 (output-weighted):')
    print(make_mix_distance(vintage).map('{:.2%}'.format).to_string())
    print('\nMake shares moving most out and back at 2022:')
    print(make_mix_movers(vintage).round(3).to_string())


def compare(old: str, new: str) -> None:
    a, b = bump_table(old), bump_table(new)
    print(f'\n== bump ratio, {old} -> {new}')
    print(
        pd.DataFrame({old: a['ours'], new: b['ours'], 'published': b['published']})
        .round(2)
        .to_string()
    )
    print('\nMake mix distance:')
    print(
        pd.DataFrame({old: make_mix_distance(old), new: make_mix_distance(new)})
        .map('{:.2%}'.format)
        .to_string()
    )


def check(vintage: str = DEFAULT_VINTAGE) -> None:
    """Invariants of the measures, on a stored vintage."""
    for y in (2017, BUMP_YEAR):
        detail_total = _use(vintage, y).to_numpy().sum()
        summary_total = summary_use(vintage, y).to_numpy().sum()
        assert np.isclose(
            detail_total, summary_total, rtol=1e-9
        ), f'{y}: summing to summary lost ${detail_total - summary_total:,.0f}'
    detail, ours = detail_own_use(vintage), summary_own_use(vintage)
    # A summary diagonal holds every detail diagonal cell under it, plus
    # off-diagonal detail cells inside the same summary block.
    assert ours.sum().ge(detail.sum() * (1 - 1e-9)).all()
    assert bump_ratio(pd.Series({2021: 1.0, 2022: 2.0, 2023: 3.0})) == 1.0
    pub = published_summary_own_use()
    assert {'484', '22', '42'} <= set(pub.index), 'summary codes missing'
    print('check passed')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--vintage', default=DEFAULT_VINTAGE)
    parser.add_argument('--compare', help='a second vintage to compare against')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        check(args.vintage)
        return
    report(args.vintage)
    if args.compare:
        report(args.compare)
        compare(args.vintage, args.compare)


if __name__ == '__main__':
    main()
