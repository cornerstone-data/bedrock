"""Give the Supply block's detail industry axis the control it does not have (#724).

``Detail_Supply_Mix_<year>`` disaggregates the published **summary** Supply block
onto a detail mix.  That summary block is its only control, so within a summary
industry group the split across detail industries is whatever the 2017 benchmark
said, carried forward untouched.  Nothing holds that axis - which is why a
commodity-side change silently reprices industry output, and why
:mod:`~bedrock.transform.iot.nowcast_targets` records ``T17`` as "the only
constraint the Supply industry columns have".

This module supplies the missing control: each detail industry column takes the
share of its summary group that **BEA's detail gross output** gives it, while
every published summary Supply cell keeps its value.

Why this is not a new commitment to BEA's series
------------------------------------------------

``T1`` already pins the **Use** industry column to exactly this quantity
(``T005 + VAPRO = GO``, ``UGO305-A``), and ``T17`` - basic plus tax wedge equals
producer - is a **hard** target.  So the balance was always going to force the
Supply column onto ``GO - T00TOP - T00SUB``.  Doing it in the seed does not adopt
a reference the model had avoided; it stops handing GRAS a correction it would
otherwise absorb entirely into the Supply interior, from where it reaches
commodity rows through the make mix.

What the residual actually was, measured before this existed:

==== ==================== =============== ===============
year gross ``T17`` $M     % of ``T005``   industries >1%
==== ==================== =============== ===============
2017 210,493              1.4%            47
2018 754,586              4.8%            344
2020 2,105,893            13.7%           382
2023 3,299,045            15.9%           366
==== ==================== =============== ===============

⚠️ **It breaks in 2018**, four years before the 2022 Economic Census mix, so the
unconstrained axis is the cause and #570's mix change only aggravates it.

⚠️ **The residual is BEA's series moving, not ours drifting.**  Decomposed into a
within-summary-group share term and a between-group level term, 2023 is **97.9%
within**; and splitting the within term by whose mix moved (half-gross, $M):

==== ========================== ============== ===================
year BEA GO mix moved vs 2017   our mix moved  standing 2017 gap
==== ========================== ============== ===================
2018 336,827                    33,498         59,824
2020 926,073                    91,259         60,336
2023 1,714,908                  128,532        81,612
==== ========================== ============== ===================

Our seed is nearly frozen, as designed; BEA reallocates within its own summary
groups by 2.1% of ``T005`` in the first year off the benchmark and 8.3% by 2023.
That is what this module imports.

Why the two controls do not fight
----------------------------------

The between-group term is small - the published summary Supply block and BEA
detail GO agree at group level to ~300,000 $M on 20.7tn in 2023 - so hitting the
GO share is compatible with preserving every summary cell.  This module takes
**only the within-group shares** from GO and leaves the group total on the
published summary Supply block.  Taking the level as well would silently swap one
BEA series for another, and the two disagree by 1.9% in 2017 and 3.0% in 2022
economy-wide, worst in retail, wholesale and transport where basic-value domestic
output and the GO series are not the same object.

⚠️ **2017 is deliberately left alone.**  There the detail split is *observed* -
it is the published benchmark - and it outranks GO's detail split, which
disagrees with it by 56,766 $M (half-gross) even in the benchmark year.
Controlling 2017 would move the anchor every section in
:mod:`~bedrock.analysis.nowcasting.sections` is scored against.  From 2018 the
Supply detail is a carry-forward and GO's is indicator-moved, so the ranking
reverses.  :data:`CONTROLLED_YEARS` is where that rule lives.

Column scaling cannot do this
------------------------------

Scaling a detail column to hit its GO target changes how much of every summary
*commodity* group that column supplies, which breaks the summary control.  The
two are simultaneously satisfiable only by a biproportional fit, which is what
:func:`fit_group` runs: within one summary industry group, alternate scaling the
detail columns to their GO targets and the *grouped* commodity rows back to the
published summary cells.  Feasible by construction - the column targets are
defined as shares of the same group total the row targets sum to.

⚠️ The prototype this replaces
(:mod:`bedrock.analysis.nowcasting.detail_go_control`) stopped on iteration count
rather than tolerance and reported implied moves too large to believe: median
9.1%, max 140%.  It reads gross output through
:func:`~bedrock.transform.iot.derived_gross_industry_output.derive_gross_output`,
which logs *"Duplicate sector codes in gross output; aggregating by sum"*.  This
module reads :func:`~bedrock.transform.iot.derived_intermediate_and_value_added.detail_gross_output_panel`
- ``UGO305-A`` summed to the 402 codes - which is the same series ``T1`` uses, so
the target here and the target the balance imposes are the same object.

The wedge is circular, and it is iterated
------------------------------------------

The column target is ``GO(producer) - T00TOP - T00SUB``, and
:func:`~bedrock.transform.iot.nowcast_va_taxes.t00top_row` allocates product
taxes onto industries using this very block's market shares.  So the target
depends on the answer.  The dependence is weak - the wedge *total* is fixed at
``TOP + MDTY``, only its industry split moves, and ``T00SUB`` does not touch the
block at all - so :func:`go_controlled_supply_block` iterates target and wedge to
a fixed point, refitting from the seed each pass rather than from the previous
answer.  Passes and tolerance are :data:`WEDGE_PASSES` and
:data:`WEDGE_TOLERANCE`; failure to settle raises rather than returning a
half-converged block.

Run::

    uv run python -m bedrock.transform.iot.nowcast_supply_go_control
    uv run python -m bedrock.transform.iot.nowcast_supply_go_control --years 2023
"""

from __future__ import annotations

import argparse
import functools
import typing as ta

import numpy as np
import pandas as pd

from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    detail_gross_output_panel,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: The years the control is applied to.  2017 is **excluded on purpose**: see the
#: module docstring - there the detail split is the published benchmark, which
#: outranks BEA's detail GO split, and every section score is anchored on it.
CONTROLLED_YEARS: tuple[int, ...] = (2018, 2019, 2020, 2021, 2022, 2023, 2024)

#: Biproportional stopping rule: the largest miss left on any target, **in USD**.
#:
#: ⚠️ **Absolute, not relative, and that is deliberate.** A relative rule cannot
#: be set sensibly here: the columns of one group span four orders of magnitude,
#: and a rule tight enough for the small ones makes the big ones chase float64
#: rounding on sums of 402 cells worth 1e13 USD.  One million USD is the
#: published tables' own rounding unit - they are printed in whole millions - so
#: a miss under it is below the resolution of the thing being fitted.
TOLERANCE_USD = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: Cap on the biproportional sweep.
#:
#: ⚠️ **500 was too low and the symptom was misleading.** Wholesale trade (``42``)
#: converges to $0 on every column but needs a few thousand sweeps to do it; at
#: 500 it reported a 1.6e-5 relative miss that looked like infeasibility and was
#: only slowness.  The sub-blocks are at most a few hundred columns, so raising
#: this is nearly free.  A group that still exhausts it is genuinely infeasible
#: and :func:`fit_block` reverts it rather than shipping a half-fit.
MAX_ITERATIONS = 20_000

#: Fixed-point sweeps over the tax wedge, and the settling tolerance in USD per
#: industry.  Three passes reach machine agreement in practice; the cap exists so
#: a pathological year raises instead of looping.
WEDGE_PASSES = 8
WEDGE_TOLERANCE = 1.0 * MILLION_CURRENCY_TO_CURRENCY

#: A column whose seed total is below this carries no pattern to redistribute, so
#: a GO target cannot be imposed on it multiplicatively.  In USD; a millionth of
#: a million-dollar cell.
DEAD_COLUMN_USD = 1.0


@functools.cache
def _industry_parent() -> dict[str, str]:
    return {
        code: parents[0]
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }


@functools.cache
def _commodity_parent() -> dict[str, str]:
    return {
        code: parents[0]
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }


@functools.cache
def raw_supply_block(year: int, download_sources_ok: bool = False) -> pd.DataFrame:
    """The uncontrolled ``Detail_Supply_Mix_<year>`` block, commodity x industry, USD.

    ⚠️ Commodity is ``SectorConsumedBy`` and industry is ``SectorProducedBy`` -
    the Supply table's rows are commodities.  Reading them the intuitive way
    round transposes the block, which still balances economy-wide and is
    therefore invisible in every total.
    """
    fbs = pd.DataFrame(
        getFlowBySector(
            f'Detail_Supply_Mix_{year}',
            download_FBAs_if_missing=download_sources_ok,
            download_FBS_if_missing=download_sources_ok,
        )
    )
    block = (
        fbs.groupby(['SectorConsumedBy', 'SectorProducedBy'])['FlowAmount']
        .sum()
        .unstack('SectorProducedBy')
        .astype(float)
    )
    return block.reindex(
        index=list(USA_2017_COMMODITY_CODES), columns=list(USA_2017_INDUSTRY_CODES)
    ).fillna(0.0)


def seed_commodity_output(
    year: int, download_sources_ok: bool = False
) -> 'pd.Series[float]':
    """``T007`` from the **uncontrolled** block, USD by commodity.

    ⚠️ **The allocators that sit upstream of this control must call this, not
    :func:`~bedrock.transform.eeio.nowcast._supply_fbs_commodity_vector`.**  Two
    of them use commodity output as a weight and are then consumed, indirectly,
    by the tax wedge this module subtracts from its own target:

    * :func:`~bedrock.transform.iot.nowcast_product_taxes.purchaser_base` -
      ``T013 + T014``, the base ``TOP``'s residual moves on;
    * :func:`~bedrock.transform.iot.nowcast_subsidies.ppp_commodity_shares` -
      the within-sector split of 2020-21 PPP.

    Routing those through the controlled block closes a cycle -
    ``T007 -> TOP -> T00TOP -> GO target -> T007`` - which raises
    ``RecursionError`` rather than converging.

    ✅ **The approximation this makes is bounded and small.** The fit preserves
    every published summary Supply cell, so ``T007`` aggregated to summary
    commodity is **identical** before and after the control; only the detail
    split within a summary commodity group moves.  Both callers use ``T007`` as a
    *relative weight* - one inside a summary sector, one inside a tax base - so
    what they see change is a redistribution within the group they are already
    normalising over.

    """
    return raw_supply_block(int(year), download_sources_ok).sum(axis=1)


def gross_output_at_basic(year: int, wedge: 'pd.Series[float]') -> 'pd.Series[float]':
    """``GO(producer) - T00TOP - T00SUB`` by detail industry, USD.

    The Supply industry column is output at *basic* prices and gross output is
    published at *producer* prices; the wedge between them is the product-tax
    rows, which live on the Use table.  That is the ``T17`` identity, and this is
    its right-hand side per industry.

    ⚠️ ``wedge`` is passed in rather than fetched, because the caller is
    iterating it - see the module docstring.  It carries the balance's sign
    convention, in which ``T00SUB`` is already negative, so both rows are
    subtracted by summing and negating once.
    """
    output = detail_gross_output_panel()[int(year)].reindex(
        list(USA_2017_INDUSTRY_CODES)
    )
    return output.astype(float) * MILLION_CURRENCY_TO_CURRENCY - wedge.reindex(
        output.index
    ).fillna(0.0)


def fit_group(
    sub_block: pd.DataFrame,
    column_targets: 'pd.Series[float]',
    row_groups: 'pd.Series[str]',
) -> tuple[pd.DataFrame, int, float]:
    """Biproportional fit of one summary industry group's sub-block.

    ``sub_block`` is commodity x (the detail industries of one summary group).
    The fit holds two things at once:

    1. each **grouped commodity row** keeps the total it has now, which is the
       published summary Supply cell for that (summary commodity, summary
       industry) pair;
    2. each **detail industry column** takes ``column_targets``.

    Returns the fitted block, the sweeps used, and the largest miss left on
    either target in USD.  A caller that gets a miss above
    :data:`TOLERANCE_USD` has an infeasible zero pattern, not a tolerance to
    loosen.

    ⚠️ **The row scaling is applied last on purpose.** When a group is infeasible
    the two constraints cannot both hold, and this ordering decides which one
    gives way: ending on the row sweep leaves the published summary cells exact
    and the column targets missed.  Constraint 1 is the one that must never be
    given up, so the failure mode is the safe one - measured at $0 row miss on
    every group in every year, including the ones that never converge.
    """
    values = sub_block.to_numpy(dtype=float).copy()
    targets = column_targets.reindex(sub_block.columns).to_numpy(dtype=float)
    codes, group_index = np.unique(
        row_groups.reindex(sub_block.index).to_numpy(), return_inverse=True
    )
    row_targets = np.zeros(len(codes), dtype=float)
    np.add.at(row_targets, group_index, values.sum(axis=1))

    worst = np.inf
    for sweep in range(1, MAX_ITERATIONS + 1):
        columns = values.sum(axis=0)
        scale = np.divide(
            targets, columns, out=np.ones_like(targets), where=np.abs(columns) > 0
        )
        values *= scale

        grouped = np.zeros(len(codes), dtype=float)
        np.add.at(grouped, group_index, values.sum(axis=1))
        row_scale = np.divide(
            row_targets,
            grouped,
            out=np.ones_like(row_targets),
            where=np.abs(grouped) > 0,
        )
        values *= row_scale[group_index][:, None]

        columns = values.sum(axis=0)
        grouped = np.zeros(len(codes), dtype=float)
        np.add.at(grouped, group_index, values.sum(axis=1))
        worst = max(
            float(np.max(np.abs(columns - targets))),
            float(np.max(np.abs(grouped - row_targets))),
        )
        if worst <= TOLERANCE_USD:
            return (
                pd.DataFrame(values, sub_block.index, sub_block.columns),
                sweep,
                worst,
            )
    return (
        pd.DataFrame(values, sub_block.index, sub_block.columns),
        MAX_ITERATIONS,
        float(worst),
    )


def fit_block(
    block: pd.DataFrame, targets: 'pd.Series[float]'
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply :func:`fit_group` to every summary industry group.

    ``targets`` is the *absolute* GO-at-basic level per detail industry; only its
    **shares within each summary group** are used, and the group total stays the
    block's own.  Returns the fitted block and a per-group diagnostic frame.
    """
    industry_parent = pd.Series(
        {code: _industry_parent()[code] for code in block.columns}, name='group'
    )
    commodity_parent = pd.Series(
        {code: _commodity_parent()[code] for code in block.index}, name='group'
    )
    fitted = block.copy()
    rows: list[dict[str, object]] = []

    for group, columns in industry_parent.groupby(industry_parent):
        members = list(columns.index)
        sub = block[members]
        total = float(sub.to_numpy().sum())
        want = targets.reindex(members).fillna(0.0).astype(float)
        seed_columns = sub.sum(axis=0)

        dead = [c for c in members if abs(float(seed_columns[c])) < DEAD_COLUMN_USD]
        unreachable = [c for c in dead if abs(float(want[c])) >= DEAD_COLUMN_USD]
        if total <= 0 or float(want.sum()) <= 0:
            skip = 'group carries no positive mass; left as seeded'
        elif unreachable:
            skip = f'{len(unreachable)} column(s) with no seed pattern: {unreachable}'
        elif (want < -DEAD_COLUMN_USD).any():
            negative = list(want.index[want < -DEAD_COLUMN_USD])
            skip = f'negative GO-at-basic target on {negative}; left as seeded'
        else:
            skip = ''

        if skip:
            rows.append(
                {
                    'group': group,
                    'industries': len(members),
                    'sweeps': 0,
                    'worst_miss_usd': np.nan,
                    'moved_usd': 0.0,
                    'note': skip,
                }
            )
            continue

        # A singleton group's one column must take the whole group total, so the
        # normalised target is that total and the fit is the identity.  Nothing
        # special is needed for it beyond not dividing by a zero ``want``, which
        # the skip above has already ruled out.
        column_targets = want * (total / float(want.sum()))
        result, sweeps, worst = fit_group(sub, column_targets, commodity_parent)

        # ⚠️ All or nothing per group. A group that exhausts MAX_ITERATIONS has a
        # zero pattern the two constraints cannot both satisfy, and the sweep
        # ends holding the summary cells rather than the column targets - so
        # what it returns is a *partly* controlled axis whose provenance nobody
        # could state. Reverting keeps the invariant that every column is either
        # on its GO share or on the seed, never between the two.
        if worst > TOLERANCE_USD:
            rows.append(
                {
                    'group': group,
                    'industries': len(members),
                    'sweeps': sweeps,
                    'worst_miss_usd': worst,
                    'moved_usd': 0.0,
                    'note': (
                        f'infeasible: {worst / MILLION_CURRENCY_TO_CURRENCY:,.0f} $M '
                        f'left on a column target after {sweeps:,} sweeps; '
                        f'reverted to seed'
                    ),
                }
            )
            continue

        fitted[members] = result
        rows.append(
            {
                'group': group,
                'industries': len(members),
                'sweeps': sweeps,
                'worst_miss_usd': worst,
                'moved_usd': float((result - sub).abs().to_numpy().sum() / 2),
                'note': '',
            }
        )
    return fitted, pd.DataFrame(rows).set_index('group')


def _wedge(year: int, block: pd.DataFrame) -> 'pd.Series[float]':
    """``T00TOP + T00SUB`` by industry, USD, allocated on *block*'s market shares.

    Imported here rather than at module scope:
    :mod:`~bedrock.transform.iot.nowcast_va_taxes` calls back into this module
    for its own block, so a top-level import is a cycle.
    """
    from bedrock.transform.iot.nowcast_va_taxes import (  # noqa: PLC0415
        va_tax_rows,
    )

    return va_tax_rows(int(year), block=block).sum(axis=0).astype(float)


@functools.cache
def go_controlled_supply_block(
    year: int, download_sources_ok: bool = False
) -> pd.DataFrame:
    """The Supply block with its detail industry axis pinned to BEA detail GO.

    Returns the seed unchanged for years outside :data:`CONTROLLED_YEARS` - 2017
    keeps the published benchmark split, which is an observation rather than a
    carry-forward.

    ⚠️ Callers must not mutate the returned frame; it is cached.
    """
    block = raw_supply_block(int(year), download_sources_ok)
    if int(year) not in CONTROLLED_YEARS:
        return block

    wedge = _wedge(year, block)
    fitted, moved = block, float('inf')
    for _ in range(WEDGE_PASSES):
        fitted, _diagnostics = fit_block(block, gross_output_at_basic(year, wedge))
        settled = _wedge(year, fitted)
        moved = float((settled - wedge).abs().max())
        wedge = settled
        if moved <= WEDGE_TOLERANCE:
            return fitted
    raise ValueError(
        f'the {year} tax wedge did not settle in {WEDGE_PASSES} passes; the last '
        f'moved {moved:,.0f} USD on some industry against a tolerance of '
        f'{WEDGE_TOLERANCE:,.0f}. T00TOP is allocated on this block\'s market '
        f'shares, so target and answer chase each other - a wedge that will not '
        f'settle means the fit is moving market shares far more than expected.'
    )


@functools.cache
def group_diagnostics(year: int, download_sources_ok: bool = False) -> pd.DataFrame:
    """Per-summary-group convergence and movement, for the report and tests."""
    block = raw_supply_block(int(year), download_sources_ok)
    wedge = _wedge(year, go_controlled_supply_block(int(year), download_sources_ok))
    _fitted, diagnostics = fit_block(block, gross_output_at_basic(year, wedge))
    return diagnostics


def report(years: ta.Iterable[int] = CONTROLLED_YEARS) -> pd.DataFrame:
    """One row per year: what the control moved, and whether it converged."""
    rows: list[dict[str, object]] = []
    for year in years:
        seed = raw_supply_block(int(year))
        fitted = go_controlled_supply_block(int(year))
        diagnostics = group_diagnostics(int(year))
        wedge = _wedge(year, fitted)
        target = gross_output_at_basic(year, wedge)
        scale = MILLION_CURRENCY_TO_CURRENCY
        residual = fitted.sum(axis=0) - target
        rows.append(
            {
                'year': year,
                'block_total_$M': float(fitted.to_numpy().sum()) / scale,
                'level_change_%': 100
                * (float(fitted.to_numpy().sum()) / float(seed.to_numpy().sum()) - 1),
                'industry_moved_$M': float(
                    (fitted.sum(axis=0) - seed.sum(axis=0)).abs().sum()
                )
                / scale,
                'commodity_moved_$M': float(
                    (fitted.sum(axis=1) - seed.sum(axis=1)).abs().sum()
                )
                / scale,
                't17_residual_$M': float(residual.abs().sum()) / scale,
                'groups_reverted': int(diagnostics['note'].astype(bool).sum()),
                'reverted_$M': float(
                    diagnostics.loc[
                        diagnostics['note'].astype(bool), 'worst_miss_usd'
                    ].sum()
                )
                / scale,
                'max_sweeps': int(diagnostics['sweeps'].max()),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', type=int, nargs='+', default=list(CONTROLLED_YEARS))
    parser.add_argument('--groups', action='store_true', help='per-group diagnostics')
    parser.add_argument('--top', type=int, default=15)
    args = parser.parse_args()
    pd.set_option('display.width', 240)

    summary = report(tuple(args.years))
    print('\nGO control on the Supply industry axis (#724)')
    print(summary.round(2).to_string())

    if args.groups:
        for year in args.years:
            diagnostics = group_diagnostics(int(year))
            print(f'\n=== {year}: groups by value moved ===')
            print(
                diagnostics.sort_values('moved_usd', ascending=False)
                .head(args.top)
                .round(3)
                .to_string()
            )
            skipped = diagnostics[diagnostics['note'].astype(bool)]
            if not skipped.empty:
                print(f'\n=== {year}: groups the control left alone ===')
                print(skipped[['industries', 'note']].to_string())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
