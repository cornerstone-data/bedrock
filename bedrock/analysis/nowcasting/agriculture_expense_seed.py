"""Moving the farm columns' 2017 Use structure on ERS farm income expenses.

❌ **Measured, and it does not work.**  See the verdict section below: the seed
scores 4 of 7 years positive and swings from -42.9% to +33.5%, because ERS
category shares correlate **0.18** with BEA's commodity shares.  ✅ **The
extractor it is built on is worth keeping and is used elsewhere.**  This module
stays as the record of a tested no-go, not as a step in the pipeline.

Step 3 (#497) seeds the intermediate block from the 2017 benchmark and carries it
on a price index.  This is the agriculture departure from that (#577), on the
source **BEA itself used**.

✅ **BEA's own account, Table C2** (`bea_2017_benchmark_sources.md`): *"Inputs to
the agriculture, forestry, fishing, and hunting industries were estimated from
BEA NIPA estimates based on USDA ERS statistics for farm income, EIA data, and
2017 Economic Census data."*  Reading our sourcing against BEA's is the rule
[[bea-benchmark-sources-c1-c2]] records; here the two agree.

⚠️ **The extractor was in bedrock and was filtering this concept away.**
``USDA_ERS_FIWS``'s parse kept ``Cash receipts`` alone, so *Intermediate product
expenses* -- published since 1910 -- looked absent.  Three silent filters were
found there; see that module.

Why this is a *mix* seed, against what #577 says
-------------------------------------------------

⚠️ **#577 says "the value is in the levels. Do not build this expecting a
shifting mix." That rationale does not survive**, for two reasons:

* **The level is already observed.**  Step 3 controls every column to
  ``GO - VAPRO``, so a source supplying a column total supplies a number the
  step already has.
* **FIWS agrees with BEA on the level anyway.**  :func:`published_agreement`
  scores FIWS's own growth against BEA's published ``111CA`` intermediate total
  at **0.988 to 1.047** across 2018-2024 -- the closest agreement of any source
  examined in this step, and unsurprising given C2.

So the value has to be the mix.  Both objects move about as much as each other:

==========================================  ================
measurement                                  dissimilarity
==========================================  ================
FIWS's own category mix, 2017 -> 2023             0.045
BEA's ``111CA`` commodity mix at 2022        0.077 / **0.059** (`N`)
==========================================  ================

⚠️ **Size the prize honestly.**  The 13 agriculture columns are **1.8% of the
block's dollars and 6.9% of its impact on** ``N``.  ⚠️ **Do not confuse that
with the 16.4% that agriculture *rows* carry**, which is what everyone buys
*from* farms and which no seed on the farm column touches.

❌ The verdict: built, measured, and it does not earn its place
----------------------------------------------------------------

❌ **:func:`agriculture_score` does not show a gain**, on the only test
available -- BEA's published ``111CA``, 2018-2024:

======  =============  =============
year     dollar gain    impact gain
======  =============  =============
2018        -6.7%         +19.1%
2019       -17.6%         -42.9%
2020       +10.5%          +4.6%
2021        +0.4%         -18.8%
2022        -5.8%         -10.5%
2023        +3.0%         +10.7%
2024        +7.3%         +33.5%
======  =============  =============

⚠️ **Four of seven years positive on each weighting, swinging from -42.9% to
+33.5%.**  That is not a seed that tracks; it is noise around zero.
:func:`leave_one_out` finds no culprit either -- ``feed`` contributes **+8.0pp**
at 2022 and **-5.9pp** at 2024, and ``livestock purchases`` **-5.1pp** then
**+46.9pp**.  A mapping that helps in one year and hurts in the next is not a
mapping problem.

❌ **The mechanism is absent, and that is the real finding.**  Comparing each
category's share movement against the matching BEA commodity row's share
movement, on the six cleanest pairs over seven years:

===============================  ==========
pair                              correlation
===============================  ==========
electricity -> ``22``                 +0.61
pesticide -> ``325``                  +0.38
petroleum fuel & oil -> ``324``       +0.27
livestock purchases -> ``111CA``      +0.16
feed -> ``311FT``                     +0.13
fertilizer -> ``325``                 **-0.38**
**pooled, 42 observations**           **+0.18**
===============================  ==========

⚠️ **ERS category shares do not track BEA's commodity shares.**  BEA uses ERS
for the farm income **levels** -- which is exactly why
:func:`published_agreement` is 0.99-1.05 -- and distributes across commodities
by its own means.  A category-share index cannot reproduce a commodity mix that
is not built from category shares.

✅ **#577's instinct was right for a reason it did not state.**  It says "do not
build this expecting a shifting mix" because the mix is *stable*; the stronger
reason is that even where it moves, it does not move *with* BEA's.

⚠️ **What this does not establish.**  The test is at ``111CA`` summary, ten
detail columns aggregated, because that is the only place a later year is
published -- detail-level mix is unobservable and could behave differently.
Fertilizer and pesticide both collapse to ``325`` at summary, so that pair
cannot be separated. And the span contains the 2022 rebenchmark.

✅ **What is worth keeping regardless**: the extractor. ``USDA_ERS_FIWS`` now
surfaces a concept it was filtering away, three silent bugs are fixed, and it
is the only source in this step reaching **2025** -- for Step 2, for
inventories, and for whatever replaces this.

The form
--------

:func:`relative_index`'s, identical to
:mod:`~.services_transport_expense_seed`'s and for the same reason.  For a FIWS
category ``k`` mapped to BEA commodities ``C``::

    seed[c, j] = Use2017[c, j] * ( fiws[k, t] / fiws[k, 2017] ) / g[t]

BEA's own level and its own split across ``C`` are preserved and ERS supplies
only *relative* movement.  ⚠️ **A constant scope difference divides out**, which
matters here because some are large: FIWS books $15.8B of repair and maintenance
where BEA's farm columns show $105M on repair commodities.  The seed reaches
what BEA books on rows ERS names, and no more.

⚠️ **``g[t]`` is the published total, not the sum of the mapped categories.**
:data:`FIWS_TOTAL` is ERS's own ``all, excl. operator dwellings``, so the
denominator is the industry's whole intermediate bill including the ~10%
this cannot place on a commodity.  Using the mapped subset instead was a live
bug in the services seed; see :func:`~.services_transport_expense_seed.industry_growth`.

⚠️ Limits that do not go away
------------------------------

⚠️ **One farm sector against ten BEA farm columns.**  ERS has no NAICS-6 crop or
livestock split, so the index moves all ten identically and can only be
validated at the ``111CA`` aggregate.  The 2017 proportions do the splitting.

⚠️ **Forestry, fishing and support (``113000``, ``114000``, ``115000``) are not
seeded.**  FIWS is farms.  ⚠️ Note ``115000`` support activities is $22.9B *of*
the farm columns as a row -- ERS's ``machine hire & custom work`` names it -- so
it is moved as a row while its own column is held.

⚠️ **2025 is an ERS forecast**, not a realized estimate (:data:`FORECAST_YEARS`).

⚠️ **Irrigation is discontinued after 2023.**  18 categories in 2024-25 against
19 before.  Its absence is not a collapse to zero and it is not propagated.

Run::

    uv run python -m bedrock.analysis.nowcasting.agriculture_expense_seed --all
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity

#: The FBA this reads.  ⚠️ Its parse filters on ``KEPT_CONCEPTS``; intermediate
#: expenses were absent from that list until 2026-08-25.
FIWS_SOURCE = 'USDA_ERS_FIWS'

#: The published concept, as it appears in ``FlowName``.
FIWS_CONCEPT = 'Intermediate product expenses'

#: National rows only.  FIWS also publishes all 51 states.
US_LOCATION = '00000'

#: ⚠️ ERS's **own** total, and the denominator of :func:`relative_index`.
#: ``excl. operator dwellings`` because farm housing is not an intermediate
#: input to farming -- and because it is the variant that reconciles: at 2017
#: farm origin + manufactured inputs + other intermediate sums to it exactly.
FIWS_TOTAL = f'{FIWS_CONCEPT}, all, excl. operator dwellings'

#: ⚠️ **Aggregates, not leaves.**  Summing these with their own members double
#: counts.  Kept out of :data:`FIWS_ITEM_TO_BEA` deliberately.
FIWS_AGGREGATES = (
    FIWS_TOTAL,
    f'{FIWS_CONCEPT}, all, incl. operator dwellings',
    f'{FIWS_CONCEPT}, farm origin',
    f'{FIWS_CONCEPT}, manufactured inputs',
    f'{FIWS_CONCEPT}, other intermediate, excl. operator dwellings',
    f'{FIWS_CONCEPT}, other intermediate, incl. operator dwellings',
    f'{FIWS_CONCEPT}, miscellaneous , excl. operator dwellings',
    f'{FIWS_CONCEPT}, miscellaneous , incl. operator dwellings',
)

#: Years ERS publishes as a **forecast** rather than a realized estimate.
FORECAST_YEARS = (2025,)

#: ⚠️ Discontinued after 2023 -- absent, not zero.  A consumer that reads the
#: absence as a collapse invents a $1.6B fall in irrigation.
FIWS_DISCONTINUED_AFTER_2023 = (f'{FIWS_CONCEPT}, miscellaneous, irrigation',)

#: ERS expense category -> the BEA detail commodities it buys.
#:
#: ⚠️ **Scope differences here are large and are meant to be.**  ERS asks what a
#: farm spent; BEA books what commodity it bought, at purchaser prices, on its
#: own row definitions.  :func:`item_scope` measures each gap.  The index form
#: divides a constant one out; what it cannot do is reach an expense BEA books
#: on a row no category names.
FIWS_ITEM_TO_BEA: dict[str, tuple[str, ...]] = {
    # Farm origin. ⚠️ Feed is mostly *manufactured* animal feed to BEA, not raw
    # grain, so it lands on 311119 as well as the grain row.
    f'{FIWS_CONCEPT}, feed': ('311119', '1111B0'),
    f'{FIWS_CONCEPT}, livestock purchases': ('1121A0', '112A00', '112300'),
    f'{FIWS_CONCEPT}, seed': ('1111A0', '111200', '111400', '111900'),
    # Manufactured inputs.
    f'{FIWS_CONCEPT}, fertilizer, lime, & soil conditioner': ('325310',),
    f'{FIWS_CONCEPT}, pesticide': ('325320',),
    f'{FIWS_CONCEPT}, petroleum fuel & oil': ('324110', '324190'),
    f'{FIWS_CONCEPT}, electricity': ('221100',),
    # Other intermediate.
    f'{FIWS_CONCEPT}, repair & maintenance, excl. operator dwellings': (
        '811100',
        '811300',
        '811400',
    ),
    f'{FIWS_CONCEPT}, machine hire & custom work': ('115000',),
    f'{FIWS_CONCEPT}, marketing, storage, & transportation': (
        '484000',
        '493000',
    ),
    f'{FIWS_CONCEPT}, miscellaneous , insurance premiums': ('5241XX', '524200'),
    f'{FIWS_CONCEPT}, miscellaneous, irrigation': ('221300',),
    f'{FIWS_CONCEPT}, general management, motor vehicle licenses': (
        '541100',
        '541200',
        '5419A0',
    ),
}

#: ⚠️ **``insurance premiums, federal`` is a subset of ``insurance premiums``**,
#: not a sibling. Mapping both would count federal crop insurance twice.
FIWS_NESTED = (f'{FIWS_CONCEPT}, miscellaneous , insurance premiums, federal',)

#: The years the seed can build.  Bounded by the FBA, which now runs to 2025.
SEED_YEARS = (2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025)

BASE_YEAR = 2017
MILLION = 1e6


def _value(frame: pd.DataFrame, row: str | int, column: str | int) -> float:
    """One cell as a float.

    ⚠️ pandas types ``.at`` as a union wide enough to include ``Timestamp`` and
    ``bytes``, so every arithmetic use of a cell fails type checking. This is
    the one place that is dealt with.
    """
    return float(np.asarray(frame.at[row, column], dtype=float))


@functools.cache
def expense_panel() -> pd.DataFrame:
    """ERS intermediate product expenses, national, in $M, by activity and year.

    ⚠️ **The FBA is ``USD``**; this divides by :data:`MILLION` to reach the $M
    the BEA Use table is in.  Getting a unit wrong is invisible in
    :func:`relative_index`, which divides it out, and wrong for any level.
    """
    frames = []
    for year in (BASE_YEAR, *SEED_YEARS):
        fba = getFlowByActivity(FIWS_SOURCE, year)
        keep = fba[
            (fba['FlowName'] == FIWS_CONCEPT) & (fba['Location'] == US_LOCATION)
        ].copy()
        frames.append(
            pd.DataFrame(
                {
                    'item': keep['ActivityProducedBy'].astype(str),
                    'year': int(year),
                    'value': keep['FlowAmount'].astype(float) / MILLION,
                }
            )
        )
    panel = pd.concat(frames, ignore_index=True)
    return pd.DataFrame(panel.groupby(['item', 'year'], as_index=False).sum())


@functools.cache
def _use_2017_detail() -> pd.DataFrame:
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        _use_2017_detail as use,
    )

    frame = use()
    frame.index = frame.index.astype(str)
    return frame


def farm_industries() -> list[str]:
    """The BEA detail farm columns this seed moves.

    ⚠️ **Farms only.**  ``113000`` forestry, ``114000`` fishing and ``115000``
    support activities are agriculture to BEA and are *not* farms to ERS, so
    they hold their 2017 columns.
    """
    return sorted(
        str(c) for c in _use_2017_detail().columns if str(c)[:3] in ('111', '112')
    )


def _wide() -> pd.DataFrame:
    return expense_panel().pivot_table(index='item', columns='year', values='value')


def industry_growth(year: int, base_year: int = BASE_YEAR) -> float:
    """Farms' whole intermediate bill, ``year`` over ``base_year``.

    ⚠️ **ERS's published total, not the sum of the mapped categories.**  About
    10% of the bill has no commodity -- the unnamed remainder inside
    *miscellaneous* -- and it still belongs in the industry's own growth.  The
    services seed had this wrong and it was a real bug.
    """
    wide = _wide()
    base = _value(wide, FIWS_TOTAL, base_year)
    return _value(wide, FIWS_TOTAL, year) / base if base else 1.0


def usable_items(year: int, base_year: int = BASE_YEAR) -> list[str]:
    """Mapped categories published for both years with a positive base."""
    wide = _wide()
    if year not in wide.columns or base_year not in wide.columns:
        return []
    return [
        item
        for item in FIWS_ITEM_TO_BEA
        if item in wide.index
        and pd.notna(wide.at[item, base_year])
        and pd.notna(wide.at[item, year])
        and _value(wide, item, base_year) > 0
    ]


def relative_index(year: int, base_year: int = BASE_YEAR) -> pd.Series:
    """Per-BEA-commodity index carrying only *relative* movement.

    Each category's ``year / base_year`` ratio divided by
    :func:`industry_growth`, so a category that grew faster than the farm input
    bill rises in share and one that grew slower falls.  The level is untouched
    -- Step 5 owns it.

    ⚠️ Where several categories land on one commodity the indices are combined
    weighted by each category's own base-year dollars.
    """
    items = usable_items(year, base_year)
    if not items:
        return pd.Series(dtype=float)
    wide = _wide()
    overall = industry_growth(year, base_year)

    numerator: dict[str, float] = {}
    denominator: dict[str, float] = {}
    for item in items:
        base = _value(wide, item, base_year)
        ratio = _value(wide, item, year) / base / overall
        for code in FIWS_ITEM_TO_BEA[item]:
            numerator[code] = numerator.get(code, 0.0) + base * ratio
            denominator[code] = denominator.get(code, 0.0) + base
    return pd.Series({c: numerator[c] / denominator[c] for c in numerator})


def agriculture_seed(year: int, base_year: int = BASE_YEAR) -> pd.DataFrame:
    """BEA's 2017 farm columns moved on the ERS index.

    ``commodity x BEA detail industry`` in $M on the benchmark Use axes, for the
    ten farm columns :func:`farm_industries` returns.

    ⚠️ **Rows no category names hold their 2017 value**, neither dropped nor
    zeroed, and ⚠️ **the column total is held** -- the column is renormalised
    back to its 2017 total, because Step 3 owns the level through ``GO - VAPRO``
    and this supplies shape only.

    ⚠️ **All ten columns move identically in the commodities ERS names**, scaled
    by their own 2017 shares, because ERS has one farm sector.
    """
    if year not in SEED_YEARS and year != base_year:
        raise ValueError(
            f'{year} is not available for the farm expense seed; observed years '
            f'are {list(SEED_YEARS)}. The FBA is bounded by USDA_ERS_FIWS.yaml.'
        )
    use = _use_2017_detail()
    columns = farm_industries()
    seed = use[columns].astype(float).copy()
    index = relative_index(year, base_year)
    touched = [code for code in index.index if code in seed.index]
    if touched:
        seed.loc[touched, :] = seed.loc[touched, :].mul(
            index.reindex(touched).to_numpy(), axis=0
        )
    totals, base_totals = seed.sum(axis=0), use[columns].sum(axis=0)
    seed = seed.div(totals.where(totals != 0, np.nan), axis=1).mul(base_totals, axis=1)
    return seed.fillna(0.0)


def item_scope() -> pd.DataFrame:
    """Each ERS category against the BEA rows it maps to, at 2017.

    ⚠️ **The argument for indexing rather than substituting.**  Where the two
    disagree by a lot, ERS is measuring a farm's outlay and BEA a commodity
    purchase, and only their *movement* is comparable.
    """
    wide = _wide()
    use = _use_2017_detail()
    column = use[farm_industries()].sum(axis=1)
    records = []
    for item, codes in FIWS_ITEM_TO_BEA.items():
        if item not in wide.index or pd.isna(wide.at[item, BASE_YEAR]):
            continue
        ers = _value(wide, item, BASE_YEAR)
        bea = float(sum(column.get(code, 0.0) for code in codes))
        records.append(
            {
                'category': item.replace(f'{FIWS_CONCEPT}, ', ''),
                'ers_$M': ers,
                'bea_$M': bea,
                'bea_rows': ','.join(codes),
                'ratio': bea / ers if ers else np.nan,
            }
        )
    return (
        pd.DataFrame(records)
        .set_index('category')
        .sort_values('ers_$M', ascending=False)
    )


def coverage() -> pd.DataFrame:
    """How much of the farm bill the mapped categories name, by year."""
    wide = _wide()
    records = []
    for year in (BASE_YEAR, *SEED_YEARS):
        if year not in wide.columns:
            continue
        items = usable_items(year)
        total = _value(wide, FIWS_TOTAL, year)
        named = float(wide.loc[items, year].sum())
        records.append(
            {
                'year': year,
                'total_$B': total / 1000,
                'named_$B': named / 1000,
                'named_%': 100 * named / total if total else np.nan,
                'categories': len(items),
                'basis': 'forecast' if year in FORECAST_YEARS else 'estimate',
            }
        )
    return pd.DataFrame(records).set_index('year')


def published_agreement() -> pd.DataFrame:
    """ERS's own growth against BEA's published ``111CA`` intermediate total.

    ⚠️ **The scope check that refused utilities.**  It compares the source's
    **whole** intermediate bill against the published column total over the same
    span -- never a mapped subset, which measures the item map instead
    ([[bedrock-service-expense-seed]]).

    ✅ **Agriculture passes it comfortably**: 0.988 to 1.047 across 2018-2024,
    against 0.63-0.89 for utilities' SAS panel on the same test.  ⚠️ Read that
    as confirmation the two describe the same industry, not as validation of the
    mix -- BEA *builds* ``111CA`` from ERS, so agreement is partly definitional.
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )

    base = summary_intermediate(BASE_YEAR)
    records = []
    for year in SEED_YEARS:
        try:
            actual = summary_intermediate(year)
        except Exception:  # noqa: BLE001 - year not published at summary
            continue
        if '111CA' not in actual.columns:
            continue
        published = float(actual['111CA'].sum()) / float(base['111CA'].sum())
        survey = industry_growth(year)
        records.append(
            {
                'year': year,
                'ers': survey,
                'bea_111CA': published,
                'ratio': survey / published if published else np.nan,
            }
        )
    return pd.DataFrame(records).set_index('year')


def agriculture_score(
    years: tuple[int, ...] = (2018, 2019, 2020, 2021, 2022, 2023, 2024),
    drop: str | None = None,
) -> pd.DataFrame:
    """Frozen 2017 against the seeded farm columns, on BEA's published summary.

    The seed is built at BEA detail, where Step 3's estimand lives, and scored
    at ``111CA`` summary, which is the only place a later year is published.
    ⚠️ **That is also the only level ERS can be validated at**, since it has one
    farm sector.

    Reported under both weightings.  ``impact`` is ``N`` -- total kg CO2e per
    dollar, direct plus indirect -- per [[bedrock-prioritize-on-n-not-d]].

    ⚠️ **The test is biased against the seed**, and harder here than anywhere
    else in this step: BEA *builds* ``111CA`` from this very source, so agreeing
    with BEA is substantially agreeing with the seed's own input. Read a gain as
    "we reproduce BEA's method", not "we beat BEA".
    """
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        summary_intermediate,
    )
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415
        _summary_intensity,
        _to_summary,
    )

    use = _use_2017_detail()
    columns = farm_industries()
    frozen_summary = _to_summary(use[columns])
    base = summary_intermediate(BASE_YEAR)
    intensity = _summary_intensity()

    records = []
    for year in years:
        try:
            actual = summary_intermediate(year)
        except Exception:  # noqa: BLE001 - year not published at summary
            continue
        if '111CA' not in actual.columns or '111CA' not in frozen_summary.columns:
            continue
        seeded_summary = _to_summary(_seed_dropping(year, drop))
        rows = [
            r for r in base.index if r in actual.index and r in frozen_summary.index
        ]
        frozen = frozen_summary['111CA'].reindex(rows).fillna(0.0)
        seeded = seeded_summary['111CA'].reindex(rows).fillna(0.0)
        truth = actual['111CA'].reindex(rows).fillna(0.0)
        if truth.sum() <= 0 or frozen.sum() <= 0 or seeded.sum() <= 0:
            continue
        truth_share = truth / truth.sum()
        row: dict[str, object] = {
            'year': year,
            'basis': 'forecast' if year in FORECAST_YEARS else 'estimate',
        }
        for weighting in ('dollar', 'impact'):
            weights = (
                intensity.reindex(rows).fillna(0.0)
                if weighting == 'impact'
                else pd.Series(1.0, index=rows)
            )
            d_frozen = float(
                (weights * (frozen / frozen.sum() - truth_share).abs()).sum() / 2
            )
            d_seeded = float(
                (weights * (seeded / seeded.sum() - truth_share).abs()).sum() / 2
            )
            row[f'{weighting}_frozen'] = d_frozen
            row[f'{weighting}_seeded'] = d_seeded
            row[f'{weighting}_gain_%'] = (
                100 * (d_frozen - d_seeded) / d_frozen if d_frozen else np.nan
            )
        records.append(row)
    return pd.DataFrame(records).set_index('year')


def _seed_dropping(year: int, drop: str | None) -> pd.DataFrame:
    """:func:`agriculture_seed`, optionally with one category held out."""
    if drop is None:
        return agriculture_seed(year)
    kept = {k: v for k, v in FIWS_ITEM_TO_BEA.items() if k != drop}
    original = dict(FIWS_ITEM_TO_BEA)
    try:
        FIWS_ITEM_TO_BEA.clear()
        FIWS_ITEM_TO_BEA.update(kept)
        return agriculture_seed(year)
    finally:
        FIWS_ITEM_TO_BEA.clear()
        FIWS_ITEM_TO_BEA.update(original)


def leave_one_out(year: int = 2022) -> pd.DataFrame:
    """Which categories carry the gain, by dropping each in turn.

    ⚠️ **Where a weak mapping shows up.**  :func:`item_scope` flags
    ``machine hire & custom work`` at a ratio of 5.01 -- ERS's $4.6B category is
    mapped onto BEA's $22.9B ``115000`` row, which covers far more than custom
    work.  If that mapping is doing harm, dropping it improves the score.
    """
    full = agriculture_score(years=(year,))
    if full.empty:
        return pd.DataFrame()
    baseline = _value(full, year, 'impact_gain_%')
    records = []
    for item in FIWS_ITEM_TO_BEA:
        without = agriculture_score(years=(year,), drop=item)
        if without.empty:
            continue
        gain = _value(without, year, 'impact_gain_%')
        records.append(
            {
                'dropped': item.replace(f'{FIWS_CONCEPT}, ', ''),
                'gain_without_%': gain,
                'contribution_pp': baseline - gain,
            }
        )
    return (
        pd.DataFrame(records)
        .set_index('dropped')
        .sort_values('contribution_pp', ascending=False)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scope', action='store_true', help='ERS vs BEA at 2017')
    parser.add_argument('--coverage', action='store_true', help='what ERS names')
    parser.add_argument(
        '--agreement', action='store_true', help='ERS vs published 111CA growth'
    )
    parser.add_argument('--score', action='store_true', help='frozen vs seeded')
    parser.add_argument(
        '--leave-one-out', action='store_true', help='which categories carry it'
    )
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = (
        args.scope
        or args.coverage
        or args.agreement
        or args.score
        or args.leave_one_out
    )
    pd.set_option('display.width', 200)

    if args.all or args.coverage or not chosen:
        print('\nHow much of the farm intermediate bill ERS names\n')
        print(coverage().round(1).to_string())
        print(
            '\n  the unnamed ~10% is the remainder inside miscellaneous, which'
            '\n  has no commodity. It stays in the denominator regardless.'
        )
    if args.all or args.agreement or not chosen:
        print("\nERS's growth against BEA's published 111CA\n")
        print(published_agreement().round(3).to_string())
        print(
            '\n  0.99-1.05 across seven years -- the closest agreement of any'
            '\n  source in this step. BEA builds 111CA from ERS, so read it as'
            '\n  confirmation of scope, not validation of the mix.'
        )
    if args.all or args.score or not chosen:
        print('\nFrozen 2017 against the ERS-seeded farm columns, on 111CA\n')
        print(agriculture_score().round(4).to_string())
        print(
            '\n  impact is N (direct + indirect). The test is biased against'
            '\n  the seed: BEA builds 111CA from this same source, so read a'
            '\n  gain as reproducing BEA method, not beating BEA.'
        )
    if args.all or args.leave_one_out:
        print('\nWhich categories carry the gain, 2022\n')
        print(leave_one_out().round(2).to_string())
        print(
            '\n  a negative contribution means the mapping is doing harm.'
            '\n  machine hire is the one to watch: item_scope puts it at 5.01,'
            '\n  so a 4.6B category is moving a 22.9B BEA row.'
        )
    if args.all or args.scope:
        print('\nEach ERS category against the BEA rows it maps to, 2017\n')
        print(item_scope().round(2).to_string())
        print(
            '\n  ratio far from 1.0 means ERS and BEA book the outlay'
            '\n  differently -- the argument for indexing, not substituting.'
        )


if __name__ == '__main__':
    main()
