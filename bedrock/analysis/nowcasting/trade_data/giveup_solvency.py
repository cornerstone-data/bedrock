"""Which side of the trade insolvency is wrong? (#769)

:func:`~bedrock.analysis.nowcasting.control_residuals.giveup_solvency` found the
finding this module explains: the 19 trade givers' margin give-up exceeds their
own output in every nowcast year, reaching **8/19 givers and -530,409 $M of
negative total supply by 2023** (re-measured on the #724 stack: 11/19 and
-389,570).  A negative ``T016`` is infeasible for the balance -- the sign locks
refuse the negative Use row ``T11`` would then demand -- so before any guard or
fix, the question is *which of the two differenced series is wrong*:

- the **give-up**, ``census margin index x trade_coverage_ratio`` frozen at
  2017, or
- the givers' **own output**, ``T007`` from the ``Detail_Supply`` block.

The referee
-----------

BEA's summary Supply tables publish the ``Trade`` column **annually 2017-2024**,
and its giver side (the negative cells on the summary trade commodities) is
BEA's own annual estimate of exactly the quantity our give-up estimates.  The
same tables publish ``T007`` for those commodities.  So both sides of our
difference have a published annual referee at summary, and the attribution is a
roll-up away:

====================  =====================================================
comparison            what a disagreement means
====================  =====================================================
our give-up vs BEA's  the census-index-times-frozen-ratio level is off
our T007 vs BEA's     the summary control or detail split is off (#724 fixed
                      the split; the summary control is published, so this
                      column should agree near-exactly by construction)
====================  =====================================================

⚠️ BEA's annual summary is an estimate, not an observation -- but for trade
margins BEA's own annual sources are the same AWTS/ARTS/AIES series ours are,
so a *disagreement in growth* is a construction difference, not a data one.
The observation-grade re-anchor is the 2022 Economic Census, which observes the
full-universe margin (merchant plus MSBO plus agents) and can re-observe the
coverage ratio frozen at 2017; see §the ratio, measured annually.

What it found, 2026-08-30, on the #776+#777 stack
--------------------------------------------------

1. ✅ **The output side agrees with the referee exactly.**  Rolled to summary,
   our ``T007`` for the five trade groups sits on BEA's published values to
   $1-2M in every year -- the summary control passes through the detail split
   untouched, confirmed in fact.  The output side is not the wrong side.

2. ❌ **The give-up side is ours, in three separable pieces.**

   - **Wholesale's frozen 1.561 overstates from 2020 on**: the implied annual
     ratio -- BEA give-up over census margin -- runs 1.561, 1.549, 1.555,
     1.520, 1.444, 1.435, 1.489 across 2017-2023.  That decline is what a
     shrinking non-merchant universe (manufacturers' sales branches, agents)
     looks like; freezing 2017 converts it into +2.7% -> +8.8% of give-up
     overstatement on ``42`` in 2020-2022.
   - **The 2023 retail AIES splice imports a rate step BEA did not take**: the
     implied retail ratio holds 1.05-1.07 through 2022 and collapses to
     **0.966** in 2023 -- the year the spliced retail margin rate stepped
     31.3% -> 34.2%.  Our 2023 retail give-up overshoots BEA's by 10.5% on
     ``445`` and 5.6% on ``4A0``.
   - **The within-kind census split disagrees hardest on motor vehicles**:
     ``441`` give-up runs +12.6% (2022) and **+29.0%** (2023) over BEA's cell
     even though the kind totals are close -- a split error, not a level one.

3. ✅ **Adopting the published group levels removes most of the infeasibility.**
   Rescaling each group's give-up to its published ``Trade`` cell (census
   detail kept for the within-group split) cuts 2023 from 11 insolvent /
   -389,570 $M to **4 / -157,731**, and 2022 to 4 / -19,439.  What remains is
   concentrated where it belongs: ``424700``/``424200``/``447000`` (energy
   price mechanics inside a group split) and ``454000`` nonstore -- the #724
   e-commerce classification question, which the EC-2022 supply-mix work above
   this in the stack is the instrument for.

Run::

    uv run python -m bedrock.analysis.nowcasting.trade_data.giveup_solvency
    uv run python -m bedrock.analysis.nowcasting.trade_data.giveup_solvency --years 2018 2023
"""

from __future__ import annotations

import argparse

import pandas as pd

from bedrock.analysis.nowcasting.control_residuals import giveup_solvency
from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.transform.iot.nowcast_trade_margins import (
    GIVER_COMMODITIES,
    census_gross_margin,
    trade_control_total,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

#: The span both our seed and the published summary tables cover.
SPAN = (2017, 2018, 2019, 2020, 2021, 2022, 2023)

#: Summary commodity codes whose published ``Trade`` cell is a give-up.
#: ``42`` is all ten wholesale givers; retail spreads over four groups.
SUMMARY_GIVER_GROUPS = ('42', '441', '445', '452', '4A0')

#: ⚠️ The summary Supply workbook's column label really is ``Trade`` -- title
#: case, no trailing space at summary (the detail table's ``'TRADE '`` trap is
#: a different workbook).
SUMMARY_TRADE_COLUMN = 'Trade'


def _summary_supply(year: int) -> pd.DataFrame:
    frame = _load_usa_summary_sut('Supply_summary', year)  # type: ignore[arg-type]
    frame.index = frame.index.astype(str).str.strip()
    frame.columns = frame.columns.astype(str).str.strip()
    return frame


def _cell(frame: pd.DataFrame, row: str, column: str) -> float:
    return float(pd.to_numeric(frame.loc[row, column], errors='coerce'))


def _giver_to_summary() -> dict[str, str]:
    parents = {
        str(code): parents[0]
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    givers = sorted({c for kind in GIVER_COMMODITIES for c in GIVER_COMMODITIES[kind]})
    return {giver: parents[giver] for giver in givers}


def referee_table(years: tuple[int, ...] = SPAN) -> pd.DataFrame:
    """Ours vs BEA published, per summary giver group and year, million USD.

    ``ours_giveup`` is the seed's give-up rolled to summary (negative);
    ``bea_giveup`` the published summary ``Trade`` cell; the two ``T007``
    columns likewise.  The give-up attribution question is answered by the
    growth of the two give-up columns; the output side by the ``T007`` pair.
    """
    to_summary = _giver_to_summary()
    rows: list[dict[str, object]] = []
    for year in years:
        solvency = giveup_solvency(int(year))
        solvency = solvency.assign(
            group=pd.Series(solvency.index, index=solvency.index).map(to_summary)
        )
        ours = solvency.groupby('group')[['T007', 'TRADE']].sum()
        published = _summary_supply(int(year))
        for group in SUMMARY_GIVER_GROUPS:
            rows.append(
                {
                    'year': year,
                    'group': group,
                    'ours_T007': float(str(ours.loc[group, 'T007'])),
                    'bea_T007': _cell(published, group, 'T007'),
                    'ours_giveup': float(str(ours.loc[group, 'TRADE'])),
                    'bea_giveup': _cell(published, group, SUMMARY_TRADE_COLUMN),
                }
            )
    frame = pd.DataFrame(rows).set_index(['year', 'group'])
    frame['T007_gap_pct'] = 100 * (frame['ours_T007'] / frame['bea_T007'] - 1)
    frame['giveup_gap_pct'] = 100 * (frame['ours_giveup'] / frame['bea_giveup'] - 1)
    return frame


def implied_coverage_ratio(years: tuple[int, ...] = SPAN) -> pd.DataFrame:
    """The coverage ratio, re-derived annually from BEA's published give-up.

    ``trade_coverage_ratio`` freezes ``BEA give-up / census margin`` at its
    2017 value (wholesale **1.561**, retail **1.061**) and multiplies it onto
    the census index every year.  This table computes the same quotient from
    the *published* give-up year by year.  A drifting quotient is the frozen
    ratio's error term, in the units it enters the build.
    """
    rows: list[dict[str, object]] = []
    for year in years:
        published = _summary_supply(int(year))
        wholesale_bea = -_cell(published, '42', SUMMARY_TRADE_COLUMN)
        retail_bea = -sum(
            _cell(published, group, SUMMARY_TRADE_COLUMN)
            for group in ('441', '445', '452', '4A0')
        )
        rows.append(
            {
                'year': year,
                'wholesale_census': census_gross_margin('wholesale', int(year)) / 1e6,
                'wholesale_bea_giveup': wholesale_bea,
                'retail_census': census_gross_margin('retail', int(year)) / 1e6,
                'retail_bea_giveup': retail_bea,
                'ours_wholesale': trade_control_total('wholesale', int(year)) / 1e6,
                'ours_retail': trade_control_total('retail', int(year)) / 1e6,
            }
        )
    frame = pd.DataFrame(rows).set_index('year')
    frame['wholesale_ratio'] = frame['wholesale_bea_giveup'] / frame['wholesale_census']
    frame['retail_ratio'] = frame['retail_bea_giveup'] / frame['retail_census']
    return frame


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', type=int, nargs='+', default=list(SPAN))
    parser.add_argument('--top', type=int, default=19)
    args = parser.parse_args()
    pd.set_option('display.width', 240)
    years = tuple(args.years)

    print('\n=== per-giver solvency on the current tree ===')
    for year in years:
        table = giveup_solvency(int(year))
        insolvent = table[table['T016_partial'] < 0]
        print(
            f'{year}: {len(insolvent)}/19 insolvent, '
            f'{insolvent["T016_partial"].sum() / 1e0:,.0f} $M negative supply, '
            f'max give-up {table["giveup_pct"].max():.1f}%'
        )

    print('\n=== ours vs BEA published, per summary giver group ($M) ===')
    print(referee_table(years).round(1).to_string())

    print('\n=== the coverage ratio, frozen vs re-derived annually ===')
    print(implied_coverage_ratio(years).round(3).to_string())
    print(
        '\nfrozen at 2017: wholesale 1.561, retail 1.061 -- the build applies '
        'these every year; the *_ratio columns are what the published tables '
        'say they actually were.'
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
