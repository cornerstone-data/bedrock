"""
Should the Supply block's industry axis be controlled to BEA's detail gross
output? (Step 4a, #570) — a prototype, not a build step.

``Detail_Supply_<year>`` attributes the published **summary** Supply block onto
a detail mix. Its only control is that summary block, so the **detail industry
axis is unconstrained**: within a summary industry group the split across detail
industries is whatever the 2017 benchmark said, carried forward untouched, while
BEA's own detail gross output series moves those same industries every year on
live indicators.

That gap is measurable and it grows:

==== ===================== ==============================
year median ratio to GO    detail industries off by >1%
==== ===================== ==============================
2017 0.9989                **86** of 401
2022 0.9957                **367** of 401
2024 1.0009                **364** of 401
==== ===================== ==============================

In the benchmark year the two series agree, which is what says they are
comparable at all — the disagreement later is *drift*, not definition. ⚠️ On a
**before**-redefinition basis only; after redefinitions the 2017 agreement is
already gone (332 of 401 off by >1%), so this comparison is only meaningful
before.

⚠️ **A second consequence, and the reason this file exists now.** Because
nothing holds the industry axis, a change to the *commodity* mix leaks into it:
moving the mix onto the 2022 Economic Census shifted detail industry output by
up to **1.44%** on 34-39 industries, purely as a side effect. It happened to
move 331 industries closer to BEA's detail GO and 70 further, but a
commodity-side change repricing industry output at all is a defect regardless of
its sign.

What this prototype does
------------------------

Refits the detail block so that **both** controls hold at once:

1. every published summary Supply cell (commodity group x industry group) keeps
   its value — this is what ``Detail_Supply`` already guarantees, and it must
   not be given up
2. each detail industry column takes the share of its summary group that BEA's
   detail GO gives it — the constraint that is currently missing

⚠️ **Column-scaling cannot do this.** Scaling a detail column to hit its GO
target changes how much of every summary *commodity* group that column supplies,
so it breaks constraint 1. The two are only simultaneously satisfiable by a
biproportional fit, which is what :func:`fit_group` runs: within one summary
industry group, RAS the sub-block against grouped row targets (the summary cells)
and column targets (the GO shares). Feasible by construction, because the column
targets are defined as shares of the same group total the row targets sum to.

⚠️ **This does not, and must not, reconcile the two BEA series' levels.** They
disagree by 1.9% in 2017 and 3.0% in 2022 economy-wide, and by more than 5% in
12 and 28 summary groups respectively — worst in retail, wholesale and transport,
where Supply's basic-value domestic output and the GO series are not the same
object. Only the **within-group shares** are taken from GO; the group total stays
BEA's summary Supply. Taking the level as well would silently swap one BEA
series for another.

⚠️ **2022 is the odd year and that is worth knowing before trusting it.** At
summary the two series are off by >1% in 59 of 71 groups in 2022 against 27 in
both 2017 and 2024. That looks like a release-vintage mismatch between the
summary SUT and GDP-by-industry rather than anything economic, and it means the
GO shares for 2022 carry more disagreement than the years either side.

⚠️⚠️ **STATUS: the fit does not yet converge and its output is not usable.**
Two things must be resolved before any of this reaches the build:

1. **The RAS stops on ``MAX_ITERATIONS``, not on tolerance.** Residual
   summary-cell disturbance is 1.3e-3 of the block, worst cell $20.9bn — small
   relative to a 45tn table but not the exact preservation constraint 1 demands.
   Structural zeros are the likely cause: a detail industry that supplies none
   of a summary commodity group cannot be scaled into it, so some groups are
   infeasible as posed. Either detect and report infeasible groups or relax to
   a least-squares fit.
2. **The implied moves are too large to believe yet** — median industry output
   shift of **9.1%**, 267 industries over 5%, max 140%. That is not a plausible
   correction to a benchmark-anchored table, so the prior suspicion is that the
   GO series is not aligned to our industry axis the way this assumes.
   ``derive_gross_output`` logs *"Duplicate sector codes in gross output;
   aggregating by sum"* — check that mapping before trusting a single number
   here.

✅ What the prototype *does* establish is the gap it was written to measure: the
within-group industry share differs from BEA's detail GO by a median of 0.53
percentage points, with **125 of 401** industries off by more than one point.
That number does not depend on the fit and stands on its own.

Run: ``uv run python bedrock/analysis/nowcasting/detail_go_control.py``
     ``uv run python bedrock/analysis/nowcasting/detail_go_control.py --year 2024``
"""

from __future__ import annotations

import argparse
import typing as ta

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.pxi_mix_test import _detail_to_summary
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.iot.derived_gross_industry_output import derive_gross_output
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_GROSS_INDUSTRY_OUTPUT_YEARS

#: ⚠️ Before redefinitions. The Supply block is a before-redefinition object, and
#: the 2017 agreement that licenses this whole comparison only exists on that
#: basis — after redefinitions, 332 of 401 industries are already off by >1% in
#: the benchmark year itself.
REDEFINITION: ta.Literal['before', 'after'] = 'before'

#: RAS stopping rule: the largest relative miss on any target, and a cap.
TOLERANCE = 1e-10
MAX_ITERATIONS = 200


def supply_block(year: int) -> pd.DataFrame:
    """The Detail_Supply FBS domestic-output block, commodity x industry, USD.

    ⚠️ Commodity is ``SectorConsumedBy`` and industry is ``SectorProducedBy`` —
    the Supply table's rows are commodities. Reading them the intuitive way
    round transposes the block, which still balances economy-wide.
    """
    fbs = pd.DataFrame(getFlowBySector(f'Detail_Supply_{year}'))
    return (
        fbs.groupby(['SectorConsumedBy', 'SectorProducedBy'])['FlowAmount']
        .sum()
        .unstack('SectorProducedBy')
        .astype(float)
        .fillna(0.0)
    )


def gross_output(year: int) -> pd.Series:
    """BEA published detail gross output for ``year``, USD."""
    return derive_gross_output(
        ta.cast(USA_GROSS_INDUSTRY_OUTPUT_YEARS, year), REDEFINITION
    )


def fit_group(
    block: pd.DataFrame, row_groups: pd.Series, column_target: pd.Series
) -> pd.DataFrame:
    """Biproportional fit of one summary industry group's sub-block.

    :param block: detail commodities x the detail industries of one summary group
    :param row_groups: detail commodity -> summary commodity, the grouping the
        published summary cells are stated on
    :param column_target: the industry output each detail column must reach

    Rows are constrained **in groups** rather than individually: the published
    control is a summary cell, so what must be preserved is the sum over each
    summary commodity's detail children, not each child. That leaves the
    within-group commodity split free, which is exactly the freedom the mix work
    spends elsewhere and must not be spent twice here.
    """
    target_rows = block.groupby(row_groups).sum().sum(axis=1)
    current = block.to_numpy(dtype=float).copy()
    groups = row_groups.reindex(block.index).to_numpy()
    order = pd.Index(target_rows.index)
    row_index = pd.Series(order.get_indexer(groups), index=block.index).to_numpy()
    wanted_rows = target_rows.to_numpy(dtype=float)
    wanted_cols = column_target.reindex(block.columns).to_numpy(dtype=float)

    for _ in range(MAX_ITERATIONS):
        # rows: scale each summary commodity's children by one factor
        actual = np.zeros_like(wanted_rows)
        np.add.at(actual, row_index, current.sum(axis=1))
        with np.errstate(divide='ignore', invalid='ignore'):
            factor = np.where(actual > 0, wanted_rows / actual, 1.0)
        current *= factor[row_index][:, None]

        # columns: scale each detail industry to its GO-implied total
        actual_cols = current.sum(axis=0)
        with np.errstate(divide='ignore', invalid='ignore'):
            factor_c = np.where(actual_cols > 0, wanted_cols / actual_cols, 1.0)
        current *= factor_c[None, :]

        miss = np.zeros_like(wanted_rows)
        np.add.at(miss, row_index, current.sum(axis=1))
        gap = np.abs(miss - wanted_rows).sum() / max(wanted_rows.sum(), 1.0)
        if gap < TOLERANCE:
            break

    return pd.DataFrame(current, index=block.index, columns=block.columns)


def controlled(year: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """The refitted block, the block as built, and BEA's detail GO."""
    block = supply_block(year)
    output = gross_output(year)
    to_summary = _detail_to_summary()
    row_groups = pd.Series(
        [to_summary.get(c) for c in block.index], index=block.index
    ).dropna()
    block = block.loc[row_groups.index]

    industry_group = pd.Series(
        [to_summary.get(i) for i in block.columns], index=block.columns
    ).dropna()

    fitted = block.copy()
    for group in sorted(set(industry_group)):
        members = list(industry_group.index[industry_group == group])
        sub = block[members]
        total = float(sub.to_numpy().sum())
        weights = output.reindex(members).fillna(0.0).clip(lower=0.0)
        if total <= 0 or weights.sum() <= 0:
            # nothing to redistribute, or GO says nothing about this group
            continue
        if len(members) == 1:
            continue  # a one-industry group is already at its own total
        fitted[members] = fit_group(sub, row_groups, total * weights / weights.sum())
    return fitted, block, output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--year', type=int, default=2022)
    args = parser.parse_args()

    fitted, block, output = controlled(args.year)
    to_summary = _detail_to_summary()

    print(f'\nDetail Supply industry axis, {args.year}')
    built = block.sum(axis=0)
    after = fitted.sum(axis=0)
    common = built.index.intersection(output.index)

    def agreement(series: pd.Series) -> tuple[float, int]:
        """Share of each summary group taken by each detail industry, vs GO."""
        group = pd.Series([to_summary.get(i) for i in common], index=common)
        ours = series.reindex(common) / series.reindex(common).groupby(group).transform(
            'sum'
        )
        theirs = output[common] / output[common].groupby(group).transform('sum')
        gap = (ours - theirs).abs().dropna()
        return float(gap.median()), int((gap > 0.01).sum())

    for label, series in (('as built', built), ('GO-controlled', after)):
        median, off = agreement(series)
        print(
            f'  {label:<15} median within-group share gap to GO {median:.5f}, '
            f'industries off by >1pt: {off}'
        )

    print(
        f'\n  grand total preserved: {fitted.to_numpy().sum() / block.to_numpy().sum():.10f}'
    )

    # ⚠️ the control is the summary x summary CELL. Grouping only the rows
    # leaves detail industries on the column axis, and those are exactly what
    # the refit redistributes -- so that version reports a large disturbance
    # for a fit that is in fact exact.
    def summary_cells(frame: pd.DataFrame) -> pd.DataFrame:
        rows = pd.Series([to_summary.get(c) for c in frame.index], index=frame.index)
        cols = pd.Series(
            [to_summary.get(i) for i in frame.columns], index=frame.columns
        )
        return frame.groupby(rows).sum().T.groupby(cols).sum().T

    disturbance = (summary_cells(fitted) - summary_cells(block)).abs()
    scale = summary_cells(block).abs().to_numpy().sum()
    print(
        f'  worst summary-cell disturbance: ${disturbance.to_numpy().max():,.0f} '
        f'({disturbance.to_numpy().sum() / scale:.2e} of the block)'
    )

    shift = ((after - built) / built.replace(0, np.nan)).dropna()
    print(
        f'\n  industry output moves: median |{shift.abs().median():.2%}|, '
        f'>1%: {int((shift.abs() > 0.01).sum())}, >5%: {int((shift.abs() > 0.05).sum())}, '
        f'max {shift.abs().max():.1%}'
    )
    commodity = (
        (fitted.sum(axis=1) - block.sum(axis=1)) / block.sum(axis=1).replace(0, np.nan)
    ).dropna()
    print(
        f'  commodity output moves: >1%: {int((commodity.abs() > 0.01).sum())}, '
        f'max {commodity.abs().max():.1%}'
    )
    print('\n  largest industry shifts:')
    biggest = (
        pd.DataFrame({'shift': shift, 'built': built.reindex(shift.index)})
        .reindex(shift.abs().sort_values(ascending=False).index)
        .head(10)
    )
    for industry, row in biggest.iterrows():
        name = str(industry)
        print(
            f'    {name:<8}{row["shift"]:>+8.1%}   {to_summary.get(name, ""):<8}'
            f'built {row["built"] / 1e9:>8,.0f}bn'
        )


if __name__ == '__main__':
    main()
