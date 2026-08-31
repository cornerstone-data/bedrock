"""What the seed owes the balance's two hard cross-block identities, year by year.

:mod:`~bedrock.analysis.nowcasting.row_control_exposure` asks what the *imports
and exports estimates* cost the Use interior, scored against the published 2017
detail SUT.  This module asks the more general question, and asks it where the
published reference does not reach::

    T11   per commodity   T016[c]  =  T019[c]
    T17   per industry    supply.col[i] + T00TOP[i] + T00SUB[i]  =  use.col[i]

⚠️ **Both sides of both identities are our own seed.**  That is the whole point:
neither needs a published answer key, so both run on **2018-2023**, where every
section in :mod:`~bedrock.analysis.nowcasting.sections` is unusable because there
is no published detail SUT to compare against.

Why the anchor year cannot be trusted here
------------------------------------------

Whole blocks are anchored on, or rescaled to, the published 2017 tables and so
reproduce 2017 by construction.  The section docstrings say so themselves --
Step 4a is "close to circular", Step 3 at 2017 is "a plumbing test".  Measured
against published 2017, these blocks score:

===================================  =========================
block                                gross \\|error\\|, 2017, $M
===================================  =========================
``TRADE`` (anchored on the give-up)                          1
``TOP`` / ``SUB`` (anchored)                                 1
``MADJ``                                                     0
``T007`` (published detail mix)                            291
Use interior (published, rescaled)                       1,017
``F01000`` PCE                                              79
all twelve government columns                                0
===================================  =========================

None of that is evidence.  Run :func:`t17_residual` on 2017 alone and it returns
**210,493 $M on $14.86T of T005** with three industries out of tolerance; run it
on 2023 and the same quantity is **3,299,045 $M** across 366 of 402.  A check
built at the anchor year reports "clean" and is worthless.

What each identity is
---------------------

``T11`` is the one Step 5 imposes hardest, and
:mod:`~bedrock.analysis.nowcasting.row_control_exposure` explains what it does to
a row: total supply is not estimated, it is a residual, so every dollar of error
in a supply or final-demand term moves that commodity's intermediate total
one-for-one and the balance converges by inflating or draining the row.

``T17`` is the *column* analogue, and it carries an extra warning.
:mod:`~bedrock.transform.iot.nowcast_targets` records that ``T1`` binds the Use
industry column and that ``T17`` is **the only constraint the Supply industry
columns have** -- "without it that whole axis is free".  So a ``T17`` residual is
not shared out between the two panels: the Supply interior absorbs all of it, and
it reaches commodity rows from there through the make mix.

⚠️ ``T17`` is computed against ``T1``'s own gross-output vector
(:func:`~bedrock.transform.iot.derived_intermediate_and_value_added.detail_gross_output_panel`,
``UGO305-A``) rather than against the Step 3 interior's column sums.  The two are
the same number by construction wherever Step 3 builds -- the interior is
rescaled to ``GO - VAPRO`` -- but reading ``T1`` directly means ``T17`` is
measurable in **2018 and 2019**, where the interior raises (#770) and the seed
route would return nothing at all.

The third check
---------------

:func:`giveup_solvency` is not an identity but a **feasibility** bound, and it is
the one finding here the balance cannot absorb at all.  Trade output essentially
*is* margin -- the 19 givers hand over 90.8-100% of their own ``T007`` in the
anchor year -- so the give-up and the output are two independently-moved series
differenced on a knife edge.  When the give-up wins, ``T016`` goes negative, and
a negative supply row demands a negative Use row that the sign locks refuse.
:func:`~bedrock.transform.iot.nowcast_trade_margins.trade_margin_column` checks
that the column sums to zero (target ``T16``) and nothing else.

Cost
----

``T17`` and the solvency bound need only the ``Detail_Supply_Mix`` FBS, the value
added and ``UGO305-A``; ``T11`` additionally builds the Use interior and the
final-demand block, which is minutes per year.  ``--check`` therefore runs the
two cheap ones across the whole span and leaves ``T11`` behind ``--t11``.

Run::

    uv run python -m bedrock.analysis.nowcasting.control_residuals --check
    uv run python -m bedrock.analysis.nowcasting.control_residuals --t17 --years 2017 2023
    uv run python -m bedrock.analysis.nowcasting.control_residuals --t11 --years 2022
    uv run python -m bedrock.analysis.nowcasting.control_residuals --solvency
"""

from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.eeio.nowcast import (
    _supply_fbs_commodity_vector,
    derive_initial_supply_bridge,
    derive_initial_U_intermediate,
    derive_initial_value_added,
    derive_initial_Y_pur,
)
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
    detail_intermediate_inputs_panel,
)
from bedrock.transform.iot.nowcast_supply_go_control import (
    go_controlled_supply_block,
    raw_supply_block,
)
from bedrock.transform.iot.nowcast_trade_margins import (
    GIVER_COMMODITIES,
    trade_margin_column,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The milestone span.  2024 is excluded: ``TRADE`` has no source for it and
#: ``census_gross_margin`` raises rather than extrapolate by default.
SPAN = (2017, 2018, 2019, 2020, 2021, 2022, 2023)

#: The anchor.  Reported alongside the span so the contrast is visible, never
#: quoted alone -- see the module docstring.
ANCHOR_YEAR = 2017

#: The five Use value-added rows, summing to ``VAPRO``.
VA_ROWS = ('V00100', 'T00OTOP', 'V00300', 'T00TOP', 'T00SUB')

#: Measured 2026-08-29 with the #724 GO control applied (branch
#: ``step4a_go_control_724``).  ``--check`` fails if a year comes in **worse**
#: than its entry, and reports any that come in better so the baseline can be
#: advanced deliberately rather than drifting.
#:
#: ``t17_pct`` is gross \|residual\| as a percent of that year's own ``T005``;
#: ``insolvent`` counts trade commodities whose ``T007 + TRADE`` is negative.
#:
#: ⚠️ The pre-control numbers, for the record (same seed, ``controlled=False``):
#: ``t17_pct`` ran 1.4 / 4.8 / 7.3 / 13.7 / 14.8 / 16.9 / 15.9 across
#: 2017-2023.  The control removes the *within-group* term; what remains is the
#: **between-group** disagreement of BEA's own summary Supply and detail GO
#: series, which peaks in 2020-2022 -- the years
#: :mod:`~bedrock.analysis.nowcasting.summary_axis_audit` shows the pinned
#: summary workbook vintage (2017-2022) diverging from the vintage the GO panel
#: is read from.  That residual is not reducible by any within-group operator.
#:
#: ⚠️ ``insolvent`` moved BOTH ways under the control: the negative mass fell
#: (2023: -530,409 -> -389,570 $M, and ``454000`` alone recovered ~146,000) but
#: three-to-four small givers tipped marginally negative as ``T007``
#: redistributed.  Solvency is #769's to guard; this baseline records the state,
#: it does not bless it.
#: ⚠️ ``insolvent`` counts strict ``T016 < 0``, which after #769's water-filling
#: cap counts only published-rounding dust (residuals of a few $M parked where
#: a group's published give-up exceeds its output by rounding).  The
#: substantive counts before the fix were 2-12 per year; the guard in
#: ``nowcast_trade_margins.check_giveup_solvency`` now fails the build itself
#: on anything beyond dust, so this baseline is a backstop, not the instrument.
#: ⚠️ The first imports mirror of #771 dropped ``S00300`` noncomparable imports
#: entirely — the bridged clean function replaced every IEA category row and
#: the fitted bridge never emits S-coded rows, so the sixteen leaves the
#: crosswalk routes to ``S00300`` (253,131-357,492 $M of supply per year,
#: #766's construction) vanished, zeroing the supply of the largest T11
#: residual row.  A rise recorded at the time (2018 8.6 -> 10.1, 2023
#: 20.4 -> 21.6) was read as an unmasking of interior-row error; most of it
#: was this bug.  With the pass-through restored the #771/#772 anchor-true
#: vectors IMPROVE the row identity against the pre-#771 numbers (2018 8.4
#: against 8.6, 2023 19.9 against 20.4).  What remains true from that
#: episode: correcting one side of a two-sided identity can surface the other
#: side's own gap, so a T11 rise under a verified-correct fix is not by
#: itself a regression — but verify the fix before writing the narrative.
#:
#: ``t11_pct`` — the supply-equals-use gap per commodity as a percent of total
#: intermediate use — joined the baseline 2026-08-30, the first time the whole
#: span was measurable on one tree (the 2018-19 interiors, the published-level
#: trade margins and the census-conditioned output all landed in one stack).
#: ⚠️ **It is the remaining pre-Step-5 blocker**: ~17.5-19.9% in 2020-2023,
#: owned by the commodity-ROW blocks (real estate rows, subsidies #784,
#: imports composition #785, the interior carry) that the stack never
#: targeted.  The stack moved it 21-23% -> 17.5-19.9%; the column identity
#: and feasibility it did target went 15.9% -> 1.4% and 8-12 infeasible
#: commodities -> 0.
BASELINE: dict[int, dict[str, float]] = {
    2017: {'t17_pct': 1.4, 'insolvent': 2, 't11_pct': 3.8},
    2018: {'t17_pct': 1.3, 'insolvent': 1, 't11_pct': 8.4},
    2019: {'t17_pct': 1.9, 'insolvent': 1, 't11_pct': 11.0},
    2020: {'t17_pct': 5.5, 'insolvent': 1, 't11_pct': 18.7},
    2021: {'t17_pct': 5.7, 'insolvent': 0, 't11_pct': 17.5},
    2022: {'t17_pct': 6.7, 'insolvent': 1, 't11_pct': 19.6},
    2023: {'t17_pct': 1.4, 'insolvent': 0, 't11_pct': 19.9},
}

#: Slack on the recorded percentages, in percentage points.  Rebuilding an FBA
#: moves these in the third significant figure; a regression worth failing on
#: moves them by whole points.
TOLERANCE_PP = 0.3


def _commodities() -> list[str]:
    return list(USA_2017_COMMODITY_CODES)


def _industries() -> list[str]:
    return list(USA_2017_INDUSTRY_CODES)


def _published_use(column: str) -> pd.Series:
    """One column of the published 2017 detail Use SUT, million USD, by commodity."""
    frame = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    frame.columns = frame.columns.str.strip()
    values = pd.to_numeric(frame.reindex(_commodities())[column], errors='coerce')
    return values.fillna(0.0).astype(float)


def _supply_interior(year: int, controlled: bool = True) -> pd.DataFrame:
    """The Supply block as commodity x industry, million USD.

    ⚠️ **Controlled by default** (#724): from 2018 the detail industry columns
    are pinned to their share of BEA detail gross output within each summary
    industry group.  ``controlled=False`` returns the raw
    ``Detail_Supply_Mix_<year>`` FBS, which is what every number recorded in
    :data:`BASELINE` before 2026-08-29 was measured on -- pass it to reproduce
    them, not to measure the build.

    ⚠️ The axes are the reverse of what the names suggest -- in the FBS the
    commodity is ``SectorConsumedBy`` and the industry ``SectorProducedBy``,
    because the Supply table's rows are commodities.  Reading them the intuitive
    way round transposes the block, which still balances economy-wide and so is
    not caught by a totals check.
    """
    block = (
        go_controlled_supply_block(int(year), True)
        if controlled
        else raw_supply_block(int(year), True)
    )
    reindexed = block.reindex(index=_commodities(), columns=_industries())
    return reindexed.fillna(0.0) / MILLION_CURRENCY_TO_CURRENCY


def _value_added(year: int) -> pd.DataFrame:
    va = derive_initial_value_added(year, download_sources_ok=True)
    return va.reindex(columns=_industries()).fillna(0.0) / MILLION_CURRENCY_TO_CURRENCY


# ------------------------------------------------------------------ T17, columns


def t17_residual(year: int, controlled: bool = True) -> pd.DataFrame:
    """Per-industry residual on the basic-to-producer identity, million USD.

    Positive means the Supply column carries more than the Use column and the
    tax wedge account for, so the balance has to drain that industry's Supply
    interior to close.
    """
    industries = _industries()
    supply = (
        _supply_interior(year, controlled).sum(axis=0).reindex(industries).fillna(0.0)
    )
    va = _value_added(year)
    wedge = va.reindex(['T00TOP', 'T00SUB']).fillna(0.0).sum(axis=0)
    output = detail_gross_output_panel()[year].reindex(industries).astype(float)
    intermediate = detail_intermediate_inputs_panel()[year].reindex(industries)

    residual = supply + wedge - output
    return pd.DataFrame(
        {
            'supply_col_basic': supply,
            'wedge': wedge,
            'GO_producer': output,
            'T005': intermediate.astype(float),
            'residual': residual,
            'pct_of_T005': 100 * residual.abs() / intermediate.replace(0, np.nan),
        }
    )


# --------------------------------------------------------------------- T11, rows


def t11_residual(year: int) -> pd.DataFrame:
    """Per-commodity residual on the supply-equals-use identity, million USD.

    ⚠️ Slow, and it **raises for 2018 and 2019** (#770):
    :func:`~bedrock.transform.eeio.nowcast.derive_initial_U_intermediate` has no
    Use interior for those years.  That is the finding, not a defect here.
    """
    scale = MILLION_CURRENCY_TO_CURRENCY
    commodities = _commodities()
    bridge = derive_initial_supply_bridge(year, download_sources_ok=True) / scale
    interior = (
        derive_initial_U_intermediate(year)
        .reindex(index=commodities, columns=_industries())
        .fillna(0.0)
        / scale
    )
    final = derive_initial_Y_pur(year, download_sources_ok=True)
    final = final.reindex(commodities).fillna(0.0) / scale

    supply = bridge['T016'].reindex(commodities).fillna(0.0)
    intermediate = interior.sum(axis=1)
    uses = final.sum(axis=1)
    residual = supply - (intermediate + uses)

    # iota is the published 2017 split, the allocation the balance will roughly
    # reproduce: the row identity is hard, the final-demand columns are softly
    # targeted and the intermediate row carries no target of its own.
    total_use = _published_use('T019')
    iota = (_published_use('T001') / total_use.replace(0, np.nan)).fillna(0.0)

    return pd.DataFrame(
        {
            'T001_seed': intermediate,
            'Y_seed': uses,
            'T016_seed': supply,
            'iota': iota,
            'residual': residual,
            'pct_of_T019': 100
            * residual.abs()
            / (intermediate + uses).replace(0, np.nan),
            'exposure': residual.abs() * iota,
        }
    )


# --------------------------------------------------------- trade margin solvency


def giveup_solvency(year: int) -> pd.DataFrame:
    """Whether each trade commodity's margin give-up fits inside its own output.

    A row with ``T016_partial`` below zero has **negative total supply**, which
    is not an accuracy problem the balance can absorb: ``T11`` would demand a
    negative Use row, the sign locks refuse it, and GRAS works multiplicatively
    on positive mass.
    """
    givers = sorted({c for kind in GIVER_COMMODITIES for c in GIVER_COMMODITIES[kind]})
    scale = MILLION_CURRENCY_TO_CURRENCY
    output_usd = _supply_fbs_commodity_vector(year, True)
    # #769: pass the output so the within-group water-filling cap applies -
    # this measures the column as the bridge builds it, not the uncapped shape.
    margin = (
        trade_margin_column(year, commodity_output=output_usd).reindex(givers) / scale
    )
    output = output_usd.reindex(givers) / scale

    table = pd.DataFrame({'T007': output, 'TRADE': margin})
    table['T016_partial'] = table['T007'] + table['TRADE']
    table['giveup_pct'] = -100 * table['TRADE'] / table['T007'].replace(0, np.nan)
    return table.sort_values('giveup_pct', ascending=False)


# ------------------------------------------------------------------------ report


def span_summary(years: tuple[int, ...] = SPAN, with_t11: bool = False) -> pd.DataFrame:
    """One row per year: both identities, and the solvency bound."""
    rows: list[dict[str, object]] = []
    for year in years:
        columns = t17_residual(year)
        solvency = giveup_solvency(year)
        insolvent = solvency['T016_partial'] < 0
        row: dict[str, object] = {
            'year': year,
            't17_gross': float(columns['residual'].abs().sum()),
            't17_pct': 100
            * float(columns['residual'].abs().sum())
            / float(columns['T005'].sum()),
            'ind_over_1pct': int((columns['pct_of_T005'] > 1).sum()),
            'ind_over_25pct': int((columns['pct_of_T005'] > 25).sum()),
            'ind_over_50pct': int((columns['pct_of_T005'] > 50).sum()),
            'insolvent': int(insolvent.sum()),
            'negative_supply': float(solvency.loc[insolvent, 'T016_partial'].sum()),
            'max_giveup_pct': float(solvency['giveup_pct'].max()),
        }
        if with_t11:
            try:
                t11 = t11_residual(year)
                row['t11_gross'] = float(t11['residual'].abs().sum())
                row['t11_pct'] = (
                    100
                    * float(t11['residual'].abs().sum())
                    / float(t11['T001_seed'].sum())
                )
            except ValueError as error:  # #770 -- no Use interior for 2018/2019
                row['t11_gross'] = np.nan
                row['t11_pct'] = np.nan
                row['t11_note'] = str(error).split('.')[0]
        rows.append(row)
    return pd.DataFrame(rows).set_index('year')


def check(summary: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Compare a :func:`span_summary` against :data:`BASELINE`.

    Returns ``(regressions, improvements)``.  An improvement is not a failure,
    but it does mean the baseline is stale -- advance it in the same change that
    earned it, so the next run measures against what is actually shipped.
    """
    regressions: list[str] = []
    improvements: list[str] = []
    for year, expected in BASELINE.items():
        if year not in summary.index:
            continue
        got = summary.loc[year]
        pct, was_pct = float(got['t17_pct']), float(expected['t17_pct'])
        if pct > was_pct + TOLERANCE_PP:
            regressions.append(f'{year} T17 {pct:.1f}% of T005, was {was_pct:.1f}%')
        elif pct < was_pct - TOLERANCE_PP:
            improvements.append(f'{year} T17 {pct:.1f}% of T005, was {was_pct:.1f}%')

        if 't11_pct' in expected and 't11_pct' in got.index:
            t11, was_t11 = float(got['t11_pct']), float(expected['t11_pct'])
            if t11 == t11:  # measured this run (--t11 passed)
                if t11 > was_t11 + TOLERANCE_PP:
                    regressions.append(
                        f'{year} T11 {t11:.1f}% of T001, was {was_t11:.1f}%'
                    )
                elif t11 < was_t11 - TOLERANCE_PP:
                    improvements.append(
                        f'{year} T11 {t11:.1f}% of T001, was {was_t11:.1f}%'
                    )

        n, was_n = int(got['insolvent']), int(expected['insolvent'])
        if n > was_n:
            regressions.append(f'{year} {n} insolvent trade givers, were {was_n}')
        elif n < was_n:
            improvements.append(f'{year} {n} insolvent trade givers, were {was_n}')
    return regressions, improvements


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--years', type=int, nargs='+', default=list(SPAN), help='years to measure'
    )
    parser.add_argument('--t17', action='store_true', help='per-industry T17 detail')
    parser.add_argument(
        '--t11', action='store_true', help='per-commodity T11 detail (slow)'
    )
    parser.add_argument(
        '--solvency', action='store_true', help='per-giver trade margin solvency'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='fail if a year is worse than the recorded baseline',
    )
    parser.add_argument('--top', type=int, default=15, help='rows to list per detail')
    args = parser.parse_args()

    years = tuple(args.years)
    pd.set_option('display.width', 250)

    summary = span_summary(years, with_t11=args.t11)
    print('\nSeed residual against the two hard identities (millions of dollars)')
    # Plain ASCII: this prints to a cp1252 console on Windows, where the warning
    # sign the docstrings use raises UnicodeEncodeError.
    print(
        f'WARNING: {ANCHOR_YEAR} is anchored on the published tables and is not '
        f'evidence -- see the module docstring\n'
    )
    print(summary.round(1).to_string())

    if args.t17:
        for year in years:
            table = t17_residual(year)
            print(f'\nT17 -- worst {args.top} industries, {year}\n')
            print(
                table.sort_values('pct_of_T005', ascending=False)
                .head(args.top)
                .round(1)
                .to_string()
            )

    if args.t11:
        for year in years:
            try:
                table = t11_residual(year)
            except ValueError as error:
                print(f'\nT11 {year}: no Use interior -- {error}')
                continue
            print(f'\nT11 -- worst {args.top} commodities by exposure, {year}\n')
            print(
                table.sort_values('exposure', ascending=False)
                .head(args.top)
                .round(1)
                .to_string()
            )

    if args.solvency:
        for year in years:
            table = giveup_solvency(year)
            print(f'\nTrade margin give-up against own output, {year}\n')
            print(table.round(1).to_string())

    if args.check:
        regressions, improvements = check(summary)
        print()
        for line in improvements:
            print(f'  better than baseline: {line}')
        if regressions:
            for line in regressions:
                print(f'  REGRESSION: {line}')
            print(
                f'\n{len(regressions)} measurement(s) worse than the '
                f'2026-08-29 baseline.\n'
            )
            return 1
        print('  no regression against the 2026-08-29 baseline\n')
    print()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
