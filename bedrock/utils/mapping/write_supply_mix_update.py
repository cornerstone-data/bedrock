"""
Move the 2017 detail Supply mix onto what the 2022 Economic Census measured.

Writes :data:`OUT`, the domestic output block — commodity x industry, million
USD — that ``Detail_Supply_Mix_2022`` and later attribute onto. It is the published
2017 detail block with the Economic Census's own 2017 -> 2022 movement applied,
column totals unchanged.

⚠️ **Writing this file is not enough.** ``BEA_Detail_Supply_PxI`` caches it as
an FBA parquet, and ``getFlowByActivity`` serves that parquet without re-reading
the csv — so a rebuilt mix does not reach ``Detail_Supply_Mix`` until the FBA is
regenerated too, and nothing in the FBS logs says otherwise. The full sequence:

1. ``uv run python bedrock/utils/mapping/write_supply_mix_update.py``
2. ``generateFlowByActivity(source='BEA_Detail_Supply_PxI', year=2022)``
3. rebuild ``Detail_Supply_Mix_2022`` / ``_2023`` / ``_2024``

Skipping step 2 silently keeps the previous block. It has already happened once.

Run: ``uv run python bedrock/utils/mapping/write_supply_mix_update.py``

Why this exists
---------------

``Detail_Supply_Mix_<year>.yaml`` disaggregates the published **summary** Supply
block onto a detail mix, and that mix was the 2017 benchmark for every year.
The 2022 Economic Census is an observation BEA has not used and the only
independent measurement of where the mix went, so from 2022 it is what the
detail split should rest on (#570).

⚠️ **Chained, not substituted.** The built mix sees only the commodities the
concordance maps and the published block sees everything, so their *levels*
differ by a large and industry-specific offset — a median ``L1`` of 0.064 in
2017, which is the concordance, not an error. Substituting the census mix
would import that offset wholesale. Applying the census's own **ratio** cancels
any part of the offset that is stable across the two vintages, which is the
same reasoning that made chaining the fair form in ``annual_mix_test.py``.

⚠️ **Column totals are preserved and the grand total is unchanged.** This block
is consumed as *attribution shares* — ``Detail_Supply_Mix`` splits each summary
cell proportionally — so only relative weights matter. Renormalising each
column to its published total keeps the change to the mix and out of the level,
where the summary control belongs.

Two guards, and why each is here
--------------------------------

⚠️ **1. A ratio may only come from a cell the census actually measured**
(:data:`MIN_CENSUS_SHARE`). A commodity holding under 0.1% of an industry's
mapped products is not an observation — the share is a rounding artefact and its
ratio is arithmetic on noise — yet it still gets multiplied into whatever share
the *published* block carries, which can be large. Ungated, ``517110`` levers a
**0.004%** census share by a factor of **104**, and ``713200`` gambling ends up
**61%** above the frozen-mix estimate off a 0.01% share in accommodation. Both
vintages must clear the floor: one is not enough, since a cell seen in 2017 and
not 2022 gives a ratio of ~0 and the reverse gives ~inf.

⚠️ **This replaced an agreement test, which was withdrawn.** The earlier guard
refused a cell where the census and the published block disagreed about its
share by more than a factor of three. Two things were wrong with it. The
threshold was a knob, not a break — the factor's distribution runs smoothly
through 3x — and applying it *per cell* inside a two-commodity support is
incoherent, because the two shares sum to one, so refusing one holds nothing
back and renormalisation pushes the movement into it anyway. More importantly it
suppressed real signal: the IT shifts this file carries are a genuine move
toward cloud infrastructure and the laundry decline is genuine, and those are
exactly the cells an agreement test fights. ✅ The validity floor makes the
narrower claim, which is the only one the data supports.

⚠️ **2. Wholesale and retail industries are excluded entirely**
(:func:`is_trade`). ``Census_EC_PxI.yaml`` records the measurement: the
2017-built concordance covers 94.4% of 2017 trade goods value but only **70.5%**
of 2022, against **98.6%** for the service industries — the NAPCS vintage
instability is concentrated in exactly the product lines the trade sectors
report. ``423100`` motor vehicle wholesale is the visible casualty: its census
own-commodity share falls from 40.6% to 9.0% between the vintages, which would
have cut its commodity output by 14.7% and handed the difference to automotive
repair. That is a recode, not a shift in what a car dealer sells.

⚠️ **3. Boundary and "absent" industries hold** — but ⚠️ **neither label means
the data is missing, and both are ours to relax.** 20 columns (2,345bn of 2017
industry output) hold because a 2022 NAICS code's 2017 parents land on different
BEA industries; that is a *classification* change, not an existence one, and the
concordance can reallocate it — imperfectly — using 2017 relative values, which
is what BEA itself does when re-basing a benchmark across NAICS vintages. The 25
"absent" columns (875bn) are worse labelled still: **the 2022 Economic Census
reaches every one of them, with 891bn of product value.** They vanish in our own
*product* concordance, not in the census. Both are held for now because the
alternatives are unbuilt, not because the observation is unavailable.

What it comes to
----------------

35 columns move, on 76 cells; 9 cells are held by the validity floor and 9
trade columns are skipped. Sixteen commodities' output moves by more than 1%.
The grand total is unchanged to ten decimal places and no cell goes negative.

⚠️ **Two of the movers rest on very thin published columns and are worth reading
before quoting.** ``339950`` signs (**−28.5% on the rebuilt 2022 table**, a 12bn
commodity, and the largest single change in the update) turns on the
census seeing advertising services go from 0.39% to 1.28% of sign
manufacturers' mapped products, levered onto a published advertising share of
21.5%; ``333514`` (+7.4%, also 12bn) is the same shape. ⚠️ Its loss is
redistributed inside the ``339`` summary group, which is why nine ``339xxx``
commodities all move about +2.1% together — that cluster is one industry's
change, not nine. Both clear the floor
honestly — the census did measure them — but a 0.39% base is at the edge of
what a ratio can carry, and raising the floor to 0.5% removes both and caps the
largest change at 7.2%. That is a judgment call left open rather than silently
taken.

⚠️ **There is no answer key for 2022 and this is not graded.** Adoption is a
decision, not a test result: the 2022 census is newer data measured on the years
we are estimating. What is measured is that the census says the mix moved a
median ``L1`` of 0.0091 against BEA's published 0.0010 on a common commodity
support (``pxi_mix_test.py --vs-published``), and that no annual survey can
improve on holding the mix between censuses (``annual_mix_test.py``). The
guards above decide *which columns* may take the new data, never whether to.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.pxi_mix_test import (
    BASE_YEAR,
    _detail_to_summary,
    built_mix,
)
from bedrock.analysis.nowcasting.sections import supply_sut_output_reference
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

OUT = 'bedrock/utils/mapping/census_pxi/supply_mix_2022.csv'

#: The census vintage the mix is moved onto.
MIX_YEAR = 2022

#: The smallest share of an industry's mapped products a commodity may hold, in
#: **both** vintages, for its ratio to be usable. Below this the census did not
#: measure the cell — the ratio is arithmetic on a rounding artefact, and it
#: still gets multiplied into whatever share the published block carries.
#:
#: ⚠️ This is a **validity** floor, not an agreement test. An earlier version
#: refused a cell where the census and the published block disagreed about its
#: share by more than a factor of three. That was withdrawn: the factor's
#: distribution runs smoothly through 3x (sorted distances in units of log3:
#: 0.91, 0.97, | 1.04, 1.06, 1.10), so the threshold was a knob rather than a
#: discovered break, and it suppressed real signal — the IT commodity shifts
#: this file now carries are a genuine move toward cloud infrastructure, and the
#: laundry decline is genuine too. What survives is the narrow claim that a
#: ratio computed on ~0 is not an observation: ungated, `517110` levers a
#: 0.004% census share by **104x**, and `713200` gambling would rise 61%.
MIN_CENSUS_SHARE = 0.001


def is_trade(industry: str, to_summary: dict[str, str]) -> bool:
    """Whether this industry's product lines are the vintage-unstable kind.

    See guard 2 in the module docstring. Matched on the BEA summary parent as
    well as the code prefix, because a detail industry's own code is not always
    a reliable guide to which sector it sits in.
    """
    return (
        to_summary.get(industry, '') in ('42', '4A0000')
        or industry.startswith('42')
        or industry.startswith('4A')
    )


def moved_block() -> tuple[pd.DataFrame, pd.DataFrame]:
    """The updated block, and a per-column record of what happened to it."""
    published = supply_sut_output_reference(BASE_YEAR) / MILLION_CURRENCY_TO_CURRENCY
    base, _ = built_mix(BASE_YEAR)
    later, boundary = built_mix(MIX_YEAR)
    present = set(later.index.get_level_values(0))
    to_summary = _detail_to_summary()

    def shares(values: pd.Series) -> pd.Series:
        return values / values.sum()

    out = published.copy()
    log: list[tuple[str, str, int, int]] = []
    for industry in sorted(set(base.index.get_level_values(0))):
        if industry not in published.columns or published[industry].sum() <= 0:
            continue
        column = published[industry]
        if industry in boundary:
            log.append((industry, 'held-boundary', 0, 0))
            continue
        if industry not in present:
            log.append((industry, 'held-absent', 0, 0))
            continue
        if is_trade(industry, to_summary):
            log.append((industry, 'held-trade', 0, 0))
            continue

        first, second = base.loc[industry], later.loc[industry]
        # ⚠️ intersect with the published column's *non-zero* cells too. A
        # commodity the census maps but the published block leaves at zero has
        # no share to scale, and including it only makes the support look wider
        # than the movement it can carry.
        common = list(
            first.index.intersection(second.index).intersection(
                column[column > 0].index
            )
        )
        if len(common) < 2:
            # one commodity is share 1.0 in both vintages: nothing to move
            log.append((industry, 'held-single', 0, 0))
            continue

        census_base = shares(first[common])
        census_later = shares(second[common])

        ratio = pd.Series(1.0, index=column.index)
        moved = guarded = 0
        for commodity in common:
            # ⚠️ both vintages must clear the floor. One is not enough: a cell
            # the census saw in 2017 and not in 2022 gives a ratio of ~0, and a
            # cell it saw only in 2022 gives one of ~inf.
            if (
                census_base[commodity] < MIN_CENSUS_SHARE
                or census_later[commodity] < MIN_CENSUS_SHARE
            ):
                guarded += 1
                continue
            ratio[commodity] = census_later[commodity] / census_base[commodity]
            moved += 1

        candidate = column * ratio
        if candidate.sum() <= 0:
            log.append((industry, 'held-degenerate', 0, guarded))
            continue
        out[industry] = candidate * (column.sum() / candidate.sum())
        log.append((industry, 'census', moved, guarded))

    record = pd.DataFrame(log, columns=['industry', 'source', 'moved', 'guarded'])
    return out, record


def main() -> None:
    published = supply_sut_output_reference(BASE_YEAR) / MILLION_CURRENCY_TO_CURRENCY
    block, record = moved_block()

    # ⚠️ assert rather than report: a column total that drifted or a negative
    # cell means the construction is wrong, and the file must not be written.
    total_ratio = block.to_numpy().sum() / published.to_numpy().sum()
    assert abs(total_ratio - 1.0) < 1e-9, f'grand total moved by {total_ratio - 1:.3g}'
    assert not (block.to_numpy() < 0).any(), 'the update produced a negative cell'

    # ⚠️ melt rather than stack: stack drops the axis names on a frame whose
    # columns came from a workbook, and the resulting frame then carries
    # 'level_0'/'level_1' instead of commodity/industry.
    long = block.reset_index(names='commodity').melt(
        id_vars='commodity', var_name='industry', value_name='million_usd'
    )
    before = published.reset_index(names='commodity').melt(
        id_vars='commodity', var_name='industry', value_name='published'
    )
    long = long.merge(before, on=['commodity', 'industry'], how='left')
    long = long[long['million_usd'] != 0.0].copy()
    long['moved'] = (long['million_usd'] - long['published']).abs() > 1e-9
    long = long.drop(columns='published')
    long.to_csv(OUT, index=False)

    counts = record['source'].value_counts().to_dict()
    output = block.sum(axis=1)
    reference = published.sum(axis=1)
    change = ((output - reference) / reference.replace(0, np.nan)).dropna()
    print(
        f'wrote {len(long)} cells over {long["industry"].nunique()} industries -> {OUT}'
    )
    print(f'  columns: {counts}')
    print(
        f'  cells moved {int(record["moved"].sum())}, held by the guard '
        f'{int(record["guarded"].sum())}'
    )
    print(
        f'  commodity output changes over 1%: {int((change.abs() > 0.01).sum())}, '
        f'largest {change.abs().max():.1%}'
    )
    print(f'  grand total ratio {total_ratio:.10f}, negatives 0')
    biggest = (
        pd.DataFrame({'change': change, 'level': reference.reindex(change.index)})
        .reindex(change.abs().sort_values(ascending=False).index)
        .head(10)
    )
    for commodity, row in biggest.iterrows():
        print(
            f'    {str(commodity):<8}{row["change"]:>+8.2%}  '
            f'({row["level"] / 1e3:,.0f}bn)'
        )


if __name__ == '__main__':
    main()
