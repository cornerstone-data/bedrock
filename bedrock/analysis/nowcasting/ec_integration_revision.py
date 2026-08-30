"""How much did BEA's own output estimates move when it absorbed EC 2017? (#724)

The EC-2022 materiality check
(:mod:`~bedrock.analysis.nowcasting.ec_manufacturing_output_check`) says the
census would move 2022 manufacturing detail by ~5% value-weighted against BEA's
extrapolation.  Whether that is alarming or ordinary depends on a prior nobody
had measured: **how much does BEA itself move output when it finally integrates
an Economic Census?**  Wes's test: BEA's data archive holds the summary tables
as published *before* the September 2023 comprehensive update — the last
release still on the 2012 benchmark, with EC 2017 not yet integrated — and the
build already pins the tables from *after*.  The difference, on the overlap
years, is BEA's own census-absorbing revision.

The two vintages
-----------------

==========  =================================================  =====================
vintage     workbook                                           basis
==========  =================================================  =====================
**pre**     ``Supply_Tables_1997-2021_SUM.xlsx`` /             2012 benchmark,
            ``Use_SUT_Framework_1997-2021_SUM.xlsx``           EC 2017 **not** in
**post**    ``Supply_Tables_2017-2022_Summary.xlsx`` /         2017 benchmark,
            ``Use_Tables_Supply-Use_Framework_2017-2022_...``  EC 2017 integrated
==========  =================================================  =====================

The pre workbooks are the **September 29, 2022 annual update** — recovered from
BEA's data archive (``apps.bea.gov/histdata``, Industry Accounts →
2022/Q2/Annual_September-29-2022 → ``AllTablesSUP.zip``; the archive UI is a
JS application but its file store serves plain URLs) and uploaded to
``gs://cornerstone-default/extract/input-data/USA_AllTablesSUP/`` under their
original names.  The post workbooks are the first release after the
comprehensive update, which the build already pins for 2017-2022 — so the pair
differs by **exactly one release step**, and that step is the comprehensive
update.

⚠️ Overlap is **2017-2021**.  There is no pre-integration 2022: that data year
first appeared in the comprehensive update itself.

⚠️ **The comprehensive update is more than the EC.**  It also carried NIPA
annual-update revisions, new source data for PCE and trade, and the 2012→2017
benchmark rebase's definitional changes.  So the revision measured here is an
*upper bound* on the EC-2017 effect alone — but it is the right prior for the
question at hand, which is "how much does a census-absorbing release move
published output".

What it measures
-----------------

Per overlap year, post/pre on the two output axes of the summary Supply block:

* each **industry column total** (basic-value industry output, 71 sectors);
* each **commodity row's ``T007``** (domestic commodity output, 73 rows).

Both as *level* shifts per year and as the shift in *2017→2021 growth* — the
growth shift is the cleaner analogue of the EC-2022 check, which is also a
growth comparison.

Run::

    uv run python -m bedrock.analysis.nowcasting.ec_integration_revision
    uv run python -m bedrock.analysis.nowcasting.ec_integration_revision --with-ec2022
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.summary_axis_audit import (
    OLD_VINTAGE,
    _load_vintage,
    _numeric,
)

#: The pre-comprehensive workbooks: September 29, 2022 annual update, the last
#: release before EC 2017 was integrated.  See the module docstring for where
#: they were recovered from.
PRE_VINTAGE = {
    'Supply': 'Supply_Tables_1997-2021_SUM.xlsx',
    'Use': 'Use_SUT_Framework_1997-2021_SUM.xlsx',
}

#: The post-comprehensive workbooks are ``summary_axis_audit.OLD_VINTAGE`` —
#: "old" in that module's frame (versus 1997-2024) is "first release after the
#: comprehensive update" in this one.
POST_VINTAGE = OLD_VINTAGE

#: Years both vintages publish.
OVERLAP_YEARS = (2017, 2018, 2019, 2020, 2021)

#: Supply-table columns that are not industries, and rows that are not
#: commodities carrying domestic output.
NON_INDUSTRY = frozenset(
    {
        'IOCode',
        'T007',
        'MCIF',
        'MADJ',
        'T013',
        'Trade',
        'Trans',
        'T014',
        'MDTY',
        'TOP',
        'SUB',
        'T015',
        'T016',
    }
)
NON_COMMODITY = frozenset({'IOCode', 'T017'})

#: BEA summary manufacturing groups, for the comparison with the EC-2022 check.
MANUFACTURING_GROUPS = (
    '311FT',
    '313TT',
    '315AL',
    '321',
    '322',
    '323',
    '324',
    '325',
    '326',
    '327',
    '331',
    '332',
    '333',
    '334',
    '335',
    '3361MV',
    '3364OT',
    '337',
    '339',
)


def _supply(vintage: dict[str, str], year: int) -> pd.DataFrame:
    return _numeric(_load_vintage(vintage['Supply'], year))


def industry_output(vintage: dict[str, str], year: int) -> 'pd.Series[float]':
    """Basic-value output per summary industry: the Supply column totals."""
    frame = _supply(vintage, year)
    commodities = [r for r in frame.index if r not in NON_COMMODITY]
    industries = [c for c in frame.columns if c not in NON_INDUSTRY]
    return frame.loc[commodities, industries].sum(axis=0)


def commodity_output(vintage: dict[str, str], year: int) -> 'pd.Series[float]':
    """Domestic commodity output: the ``T007`` column."""
    frame = _supply(vintage, year)
    rows = [r for r in frame.index if r not in NON_COMMODITY]
    return frame.loc[rows, 'T007']


def revision(year: int, axis: str = 'industry') -> pd.DataFrame:
    """Post/pre per code for one year, on one output axis."""
    reader = industry_output if axis == 'industry' else commodity_output
    pre = reader(PRE_VINTAGE, year)
    post = reader(POST_VINTAGE, year)
    codes = [c for c in post.index if c in pre.index]
    frame = pd.DataFrame({'pre': pre[codes], 'post': post[codes]})
    frame['shift_pct'] = 100 * (frame['post'] / frame['pre'].replace(0, np.nan) - 1)
    return frame


def growth_revision(axis: str = 'industry') -> pd.DataFrame:
    """The 2017→2021 growth per code, pre vs post — the EC-2022 check's analogue.

    A level shift common to 2017 and 2021 cancels here, so what remains is the
    part of the revision that re-shaped the *path* — which is what our
    growth-based EC-2022 comparison would have caught, had it been run on the
    pre vintage.
    """
    reader = industry_output if axis == 'industry' else commodity_output
    first, last = OVERLAP_YEARS[0], OVERLAP_YEARS[-1]
    pre_g = reader(PRE_VINTAGE, last) / reader(PRE_VINTAGE, first).replace(0, np.nan)
    post_g = reader(POST_VINTAGE, last) / reader(POST_VINTAGE, first).replace(0, np.nan)
    codes = [c for c in post_g.index if c in pre_g.index]
    frame = pd.DataFrame({'g_pre': pre_g[codes], 'g_post': post_g[codes]})
    frame['weight'] = reader(POST_VINTAGE, last)[codes]
    frame['growth_shift_pct'] = 100 * (frame['g_post'] / frame['g_pre'] - 1)
    return frame


def _stats(frame: pd.DataFrame, column: str, weights: 'pd.Series[float]') -> str:
    values = frame[column].abs()
    weighted = np.average(
        values.dropna(), weights=weights.reindex(values.dropna().index)
    )
    counts = {t: int((values > t).sum()) for t in (1, 2, 5)}
    return (
        f'weighted mean |shift| {weighted:5.2f}%   '
        f'>1%: {counts[1]:>2}  >2%: {counts[2]:>2}  >5%: {counts[5]:>2}  of {values.notna().sum()}'
    )


def report(with_ec2022: bool = False, top: int = 10) -> None:
    print(
        '\nBEA output revision when EC 2017 was integrated '
        '(Sept 2022 vintage -> first post-comprehensive vintage)'
    )
    for axis in ('industry', 'commodity'):
        print(f'\n=== {axis} output, level shift by year ===')
        for year in OVERLAP_YEARS:
            frame = revision(year, axis)
            total = 100 * (frame['post'].sum() / frame['pre'].sum() - 1)
            print(
                f'  {year}: total {total:+5.2f}%   '
                + _stats(frame, 'shift_pct', frame['post'])
            )
        frame = revision(OVERLAP_YEARS[-1], axis)
        print(f'  worst {top} at {OVERLAP_YEARS[-1]}:')
        print(
            frame.sort_values('shift_pct', key=lambda s: s.abs(), ascending=False)
            .head(top)
            .round(2)
            .to_string()
        )

        growth = growth_revision(axis)
        print(f'\n=== {axis} 2017->2021 growth shift (level component cancelled) ===')
        print('  ' + _stats(growth, 'growth_shift_pct', growth['weight']))
        print(
            growth.sort_values(
                'growth_shift_pct', key=lambda s: s.abs(), ascending=False
            )
            .head(top)
            .round(3)
            .to_string()
        )

    manufacturing = growth_revision('industry').loc[
        lambda f: f.index.isin(MANUFACTURING_GROUPS)
    ]
    print('\n=== manufacturing groups: BEA\'s own EC-2017 growth revision ===')
    print(
        manufacturing.round(3)
        .sort_values('growth_shift_pct', key=lambda s: s.abs(), ascending=False)
        .to_string()
    )
    print(
        '  manufacturing weighted mean |growth shift|: '
        f'{np.average(manufacturing["growth_shift_pct"].abs(), weights=manufacturing["weight"]):.2f}%'
    )

    if with_ec2022:
        from bedrock.analysis.nowcasting.ec_manufacturing_output_check import (  # noqa: PLC0415
            implied_bea_growth,
        )
        from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (  # noqa: PLC0415
            load_bea_v2017_industry_to_bea_v2017_summary,
        )

        detail = implied_bea_growth()
        parents = {
            code: parent[0]
            for code, parent in load_bea_v2017_industry_to_bea_v2017_summary().items()
        }
        grouped = (
            detail.assign(g=pd.Series(detail.index, index=detail.index).map(parents))
            .groupby('g')[['r17', 'r22', 'go17', 'go22']]
            .sum()
        )
        ec22 = 100 * (
            (grouped['r22'] / grouped['r17']) / (grouped['go22'] / grouped['go17']) - 1
        )
        side = pd.DataFrame(
            {
                'bea_ec17_growth_revision_%': manufacturing['growth_shift_pct'],
                'our_ec22_implied_diff_%': ec22.reindex(manufacturing.index),
            }
        ).dropna()
        print(
            '\n=== side by side: what BEA did for EC 2017 vs what EC 2022 implies ==='
        )
        print(
            side.round(2)
            .sort_values(
                'our_ec22_implied_diff_%', key=lambda s: s.abs(), ascending=False
            )
            .to_string()
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--with-ec2022',
        action='store_true',
        help='append the manufacturing side-by-side with the EC-2022 check',
    )
    parser.add_argument('--top', type=int, default=10)
    args = parser.parse_args()
    pd.set_option('display.width', 240)
    report(with_ec2022=args.with_ec2022, top=args.top)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
