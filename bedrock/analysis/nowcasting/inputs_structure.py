"""What can be sourced for manufacturing's intermediate input column?

Step 3 seeds the Use table's intermediate block from the 2017 benchmark and
carries it forward (#497).  For manufacturing, the part of that column the
annual surveys cannot refresh is the materials bill: AIES publishes all
"materials, parts and supplies (not for resale)" as **one cell**, 82.5% of the
column, so only 8.3% of manufacturing's intermediate is commodity-mappable
annually (#564).

⚠️ **This module was ``materials_structure.py`` and is now named for the column
rather than for the biggest piece of it.**  Measured against the 2017 detail Use
table over 232 manufacturing BEA detail industries -- a $3.567T column -- the
sources read here reach **85.8% of it**:

============================================ =======
``Census_EC_MatFuel`` materials                79.4%
named non-materials cells, and seedable         6.4%
reached as expense, but not one commodity      10.0%
neither                                         4.2%
============================================ =======

⚠️ **An earlier draft of this table said 11.7% and 91.0%, and both were too
generous.**  The 11.7% counted survey-side dollars, and two of its largest
entries are not purchases of a commodity at all: resales (3.6%) are goods bought
and sold on untransformed, which the Use table handles through trade margins,
and contract work (1.4%) is manufacturing services whose commodity is the
buyer's own industry rather than any fixed row.  Adding the survey's own
residual, "all other operating expenses" (5.1%), the honest split is the one
above: **6.4% of the column is both named and placeable on a BEA commodity**,
and 85.8% is reachable in total rather than 91.0%.

The 6.4% is electricity, repair, temporary staff, professional and technical
services, advertising, refuse, data processing, communication, expensed software
and expensed computers -- ten cells, each mapping onto a BEA commodity, worth
$228.6B of a $3,566.6B column.

✅ **That block is now seeded**, by :func:`nonmaterial_seed`, and the form is an
**index rather than a level**.  :func:`expense_scope` is why: at the 2017 base
itself the survey and BEA disagree about what these cells contain by factors of
**0.40 to 8.01**, and indexing cancels every one of those while substituting the
level would import them all.

The commodity breakout is quinquennial Economic Census, and **2022 is a second
observation of it** between the benchmark and the end of the nowcast span.  The
measurements ask whether that second observation is worth having, and what has
to be built around it before it can be used:

1. ``--coverage`` -- **how much of the materials bill can be placed on a BEA
   commodity at all?**  A source that is mostly residual buckets cannot inform a
   commodity mix no matter how often it is published.
2. ``--movement`` -- **how much did the materials mix actually move, 2017 to
   2022?**  If it barely moved, a frozen 2017 structure was already right and
   the second observation buys nothing.  Scored with the same index of
   dissimilarity :mod:`~.intermediate_structure_drift` uses, so the two numbers
   are directly comparable.  Prints two figures: the full frame, and the
   **unsuppressed subsample**, which is the one to quote (see
   :func:`clean_movement`).
3. ``--where`` -- **which industries moved**, so the effort has a target list.
4. ``--holdout`` and ``--recovery`` -- **how much of any of this is the
   suppression fill rather than the economy.**  Ask before believing a column.
5. ``--groups`` -- **the within-group split**, and a holdout that scores it.
   :func:`place_on_commodities` divides a ``group`` cell over the BEA
   commodities it could be, on 2017 Use shares, and
   :func:`commodity_movement` re-scores the mix on the commodity frame Step 3
   actually seeds rather than on MATFUEL codes.
6. ``--vintage`` -- **the 2017/2022 code revision**, and how much of it survives
   a proper concordance.  :func:`vintage_diff`.
7. ``--annual`` -- **is a linear interpolation between the two censuses even
   defensible?**  :func:`annual_path` scores it against the observed annual
   path, from ``Census_ASM_Expenses`` (2018-2021) and ``Census_AIES_Expenses``
   (2023).  It is not -- for the *level*, which those surveys observe.
8. ``--form`` -- **so what form does the MIX take between the two censuses?**
   :func:`materials_theta` rejects the price-carried path the plan proposed,
   :func:`interior_form_holdout` chooses the interior form on the benchmark
   panel, and :func:`extrapolation_holdout` settles the tail.  See below.
9. ``--seed`` -- **the built seed**, :func:`materials_seed`, and what it moves.

The interpolation form, and why the summary panel cannot choose it
-----------------------------------------------------------------

The annual surveys observe the materials **level** every year, so nothing has to
be interpolated there and ``--annual`` rules out doing so.  The **mix** is a
different matter: the census breaks it out only at 2017 and 2022, and no source
observes the interior at all -- the fuels share of the census universe sits
between 0.98% and 1.14% throughout (:func:`annual_partition`), so the coarse
annual partition constrains the commodity mix hardly at all.

❌ **The price-carried path the plan named is rejected** (:func:`materials_theta`).
Carrying the 2017 census mix to 2022 on ``(p_c(2022)/p_c(2017)) ** theta`` fits
**theta = 0.00** on the unsuppressed frame -- the one :func:`clean_movement` says
to quote -- and theta = -0.25 for +0.4% on the full one.  **The level moves with
price and the mix does not**, so the reasoning that carried #497's price index
into this question does not survive contact with the census span.

⚠️ **The published summary panel cannot arbitrate the form, and the reason is
structural.**  BEA's annual tables are the last benchmark carried forward on
annual indicators -- and since BEA has not incorporated the 2022 Economic Census,
its 2022 and 2023 tables are still **2017**-benchmark carries.  So every interior
year of a summary span is itself an interpolation, and scoring a candidate
against it measures agreement between two methods rather than the shape of the
thing.  ⚠️ Any test of interpolation form run on that panel is circular, however
clean the arithmetic looks.

✅ **The benchmark detail panel can arbitrate it**, because 2007, 2012 and 2017
are three independent Economic-Census-anchored observations.  Interpolating
2007 -> 2017 and scoring at the observed 2012 (:func:`interior_form_holdout`),
on manufacturing:

======================  =============
form                    dissimilarity
======================  =============
frozen at 2007                 0.0889
linear                         0.0764
**geometric**                  **0.0710**
endpoint (the 2017 mix)        0.1216
======================  =============

✅ **Interpolating beats freezing by 20.1%**, ✅ **geometric beats linear by
7.1%**, and ❌ **adopting the newer observation early is worse than freezing** --
so "just use the 2022 census for every year" is the one candidate that loses to
doing nothing.

❌ **But do not extend the trend past the last observation**
(:func:`extrapolation_holdout`).  From 2007 and 2012, reaching the observed 2017,
holding the 2012 mix scores 0.1232 against 0.1569 for a linear trend and 0.1513
for a geometric one -- **27.4% worse on manufacturing, 41.7% on the whole
table**.  A mix trend does not persist.

**So S3 ships an asymmetry**: geometric interpolation between 2017 and 2022,
and the 2022 mix held flat for 2023 and 2024.  ⚠️ **One clean interior
observation on a ten-year span**, transferred to the census's five-year one; it
is the only clean test the data admits, not a large sample.

The bridge to BEA
-----------------

``MATFUEL`` codes are 8-digit and NAICS-derived -- ``33110090`` iron and steel
ingot, ``33272203`` bolts and nuts -- so the longest NAICS prefix that appears
in ``NAICS_to_BEA_Crosswalk_2017.csv`` resolves the material.  The
seller-not-maker problem that made the trade concordance hard does not arise
here, because a material is defined by what it *is*.

Three tiers come out of that, and the distinction is the whole story:

``direct``
    the prefix reaches a NAICS that maps 1:1 onto one BEA detail commodity.
``group``
    the prefix only reaches a NAICS covering several BEA detail commodities
    (``322`` paper, for instance).  Placeable, but needs a within-group split --
    2017 Use shares are the obvious source, and that is a much lighter benchmark
    dependency than freezing the whole column.
``residual``
    :data:`~bedrock.extract.census.Census_EC.MATFUEL_RESIDUAL_CODES` -- real
    spend Census could not place.  The ceiling on this source.

⚠️ **``00772000`` "Total Materials" is the industry total and is excluded
everywhere here.**  The named codes sum to it exactly, so leaving it in doubles
the table.  See :func:`~bedrock.extract.census.Census_EC.census_EC_MatFuel_parse`.

⚠️ **The vintages sit on different NAICS bases** (``NAICS2017`` / ``NAICS2022``)
and share 345 industries by raw code equality.  ``--movement`` scores only that
shared frame, rather than reading an absent code as a fall to zero.

✅ **But the frame does not have to be shrunk, and ``--vintage`` shows why.**
The *material* axis is not affected at all -- 289 of 289 and 290 codes are
common -- and every off-frame dollar on the industry axis is NAICS 2022 merging
2017 codes, chiefly ``336111`` + ``336112`` -> ``336110``.  Summing the 2017
side to the merged unit puts **100% of both vintages** on one basis with no
split assumption.  :func:`_common_industry_basis` is that reconciliation, and
:func:`commodity_movement` and :func:`annual_path` both run on it.

✅ **Withheld cells are recovered, not left at zero.**  Census withholds 9.1% of
2017 cells and 7.5% of 2022 ones.  The FBA records the flag and zeroes the value;
:func:`~bedrock.extract.census.Census_EC.estimate_suppressed_ec_matfuel` then
fills them against each industry's published total, which closes every one of
the 406 (2017) and 386 (2022) industry-by-kind controls to within 0.1%.  Pass
``recover=False``, or ``--recovery``, to see what that changed: $39.9B in 2017
and $87.5B in 2022.

⚠️ **But a recovered cell is a placement, not a measurement, and ``--holdout``
says how rough: WAPE 0.60 and 0.72.**  The mass is exact -- the residual is fixed
by the published total -- so all of that error is *allocation across materials
within the column*, which is precisely what a mix score measures.  So the full
frame's 0.1588 is contaminated, and :func:`clean_movement`, restricting to the
193 industries with nothing withheld in either year, gives **0.1330** with 17
columns over 0.25 instead of 66.  Quote the clean figure; the extremes in the
full one are mostly the fill.

⚠️ **And 0.1330 is a MATFUEL-code score, not a commodity one.**  Step 3 seeds
BEA detail commodities, and on that frame -- 289 materials aggregated onto ~200
commodities, industries reconciled -- the same clean subsample scores
**0.0949** (:func:`commodity_movement`).  That is the number comparable to
:mod:`~.intermediate_structure_drift`'s 0.173 for the whole Use column over an
equal-length span, because both are BEA detail commodity frames.  So the
materials block moves **roughly half as much as the column as a whole**, which
is a weaker claim than a naive 0.133-against-0.173 reading gives.

⚠️ **BEA has not used the 2022 Economic Census yet.**  Its 2022 and 2023 summary
tables are still annual-survey updates over the 2017 benchmark, so anything
here is *new information relative to the published table* rather than something
to reconcile against it.  Aggregating a census-seeded intermediate block to
summary and differencing it against BEA's 2022 Use is therefore not a
validation, and a gap is not evidence of an error on this side.

The annual path, and why it is not a straight line
-------------------------------------------------

Interpolating linearly between the two censuses is the obvious first form, and
``--annual`` is what rules it out.  Manufacturing's materials bill is observed
every year the census misses -- ASM to 2021, AIES from 2023 -- and the observed
path is **V-shaped, then falling**::

    2017 census   2,830      2021 ASM      3,109
    2018 ASM      3,053      2022 census   3,772
    2019 ASM      2,966      2023 AIES     3,517
    2020 ASM      2,636

⚠️ **Interpolating overstates 2020 by 28.8%**, a per-industry WAPE of 0.308,
because a straight line between two points five years apart cannot bend around a
pandemic.

⚠️ **Extrapolating past 2022 gets the sign wrong.**  The line says 3,961 for
2023 and the survey says 3,517 -- a fall of 6.8% read as a rise of 5.0%.  That
is the span the nowcast leans on hardest, and it is the span the straight line
fails worst.

⚠️ **The observed span ends at 2023** (:func:`unobserved_years`).  ASM ends at
2021, the census is quinquennial, and AIES 2024 still returns ``204 No Content``.
So the form is fitted and scored on observed years only; extending the span to
2024 is #707 and is Phase 2 work, and **2025 is out of scope**.

✅ **What the annual surveys buy is the level and the coarse partition** --
materials, fuels, electricity, contract work, resales and ten to thirteen named
purchased services.  ⚠️ **They do not buy the commodity mix**, which remains a
two-point interpolation; only its *form* is now an empirical question rather
than a default.

⚠️ **BEA has not incorporated the 2022 Economic Census.**  Its 2022 and 2023
summary tables are still annual-survey updates carried over the 2017 benchmark,
so everything measured here is *new information relative to the published
table*, not something to reconcile against it.  Aggregating a census-seeded
intermediate block to summary and differencing it against BEA's 2022 Use is
therefore **not a validation**, and a gap is not evidence of an error on this
side.  This bites hardest on Intermediate Uses and Intermediate Supply, which is
exactly what Step 3 builds.

Run::

    uv run python -m bedrock.analysis.nowcasting.inputs_structure
    uv run python -m bedrock.analysis.nowcasting.inputs_structure --all
"""

from __future__ import annotations

import argparse
import functools

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.intermediate_structure_drift import (
    BENCHMARK_YEAR,
    benchmark_detail_intermediate,
    column_shares,
    dissimilarity,
)
from bedrock.extract.census import Census_EC
from bedrock.extract.census.Census_AIES import (
    AIES_EXPENSE_CONTROLS,
)
from bedrock.extract.census.Census_ASM import (
    ASM_EXPENSE_CONTROLS,
    ASM_NON_COMMODITY,
)
from bedrock.extract.census.Census_EC import (
    MATFUEL_RESIDUAL_CODES,
    MATFUEL_SCRAP_BEA_CODE,
    MATFUEL_SCRAP_CODES,
    MATFUEL_TOTAL_CODES,
    estimate_suppressed_ec_matfuel,
)
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.transform.iot.nowcast_intermediate import (
    carry_shares,
    commodity_price_factor,
)

#: Same file :mod:`~.pxi_mix_test` reads, and by the same repo-relative path.
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: The 2017 <-> 2022 NAICS concordance, which is what makes the two census
#: vintages comparable at all.  See :func:`_common_industry_basis`.
NAICS_YEAR_CONCORDANCE = 'bedrock/utils/mapping/naics/NAICS_Year_Concordance.csv'

#: The two Economic Census vintages either side of the nowcast span's midpoint.
VINTAGES = (2017, 2022)

#: The benchmark detail SUT panel's three years -- three INDEPENDENT
#: Economic-Census-anchored observations, which is what makes them the only
#: clean arbiter of the interpolation form.  See :func:`interior_form_holdout`.
BENCHMARK_PANEL_YEARS: tuple[BENCHMARK_YEAR, ...] = (2007, 2012, 2017)

BILLION = 1e9


@functools.cache
def _naics_to_bea() -> tuple[dict[str, str], set[str]]:
    """``NAICS -> BEA detail`` where unambiguous, plus the ambiguous NAICS set.

    A NAICS that covers several BEA detail commodities cannot place a material
    on its own; it is kept separately so a ``group`` tier can be reported rather
    than silently counted as unmapped.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    per_naics = crosswalk.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    unique = crosswalk[
        crosswalk['NAICS_2017_Code'].isin(per_naics[per_naics == 1].index)
    ]
    return (
        unique.drop_duplicates('NAICS_2017_Code')
        .set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']
        .to_dict(),
        set(per_naics[per_naics > 1].index),
    )


def classify(material: str) -> tuple[str, str | None]:
    """``(tier, bea_detail_code)`` for one 8-digit ``MATFUEL`` code.

    ⚠️ **Purchased scrap is resolved before the prefix walk, and has to be.**
    Every scrap code begins ``33``, which reaches no single BEA commodity, so the
    walk would file $29.6B (2017) and $54.3B (2022) into the bare ``33`` group of
    136 commodities and smear it across all of manufacturing.  BEA carries
    ``S00401`` Scrap for exactly this, and Census's "excluding home scrap" is the
    same concept -- bought in rather than generated on site.
    """
    if material in MATFUEL_RESIDUAL_CODES:
        return 'residual', None
    if material in MATFUEL_SCRAP_CODES:
        return 'direct', MATFUEL_SCRAP_BEA_CODE
    unique, ambiguous = _naics_to_bea()
    for length in (6, 5, 4, 3, 2):
        prefix = material[:length]
        if prefix in unique:
            return 'direct', unique[prefix]
        if prefix in ambiguous:
            return 'group', None
    return 'unplaced', None


@functools.cache
def materials(year: int, recover: bool = True) -> pd.DataFrame:
    """The ``Census_EC_MatFuel`` FBA for one vintage, totals removed, classified.

    ``ActivityProducedBy`` is the material and ``ActivityConsumedBy`` the
    consuming NAICS-6 industry -- the Use table's own orientation.

    ``recover`` runs
    :func:`~bedrock.extract.census.Census_EC.estimate_suppressed_ec_matfuel`
    first, which fills the withheld cells against each industry's published
    total and drops the control rows.  On by default, because leaving 412
    withheld cells at zero biases a materials mix toward whatever happens to be
    publishable -- systematically the large materials.  ``False`` is for
    measuring what the recovery changed.
    """
    fba = getFlowByActivity('Census_EC_MatFuel', year)
    fba = (
        estimate_suppressed_ec_matfuel(fba)
        if recover
        else fba[~fba['ActivityProducedBy'].isin(MATFUEL_TOTAL_CODES)].copy()
    )
    tiers = fba['ActivityProducedBy'].map(classify)
    fba['tier'] = [tier for tier, _ in tiers]
    fba['bea'] = [code for _, code in tiers]
    return fba.rename(
        columns={'ActivityProducedBy': 'material', 'ActivityConsumedBy': 'industry'}
    )


def coverage(recover: bool = True) -> pd.DataFrame:
    """How much of each vintage's materials bill can be placed on a commodity."""
    records = []
    for year in VINTAGES:
        fba = materials(year, recover=recover)
        total = float(fba['FlowAmount'].sum())
        by_tier = fba.groupby('tier')['FlowAmount'].sum()
        row: dict[str, object] = {'year': year, 'published_$B': total / BILLION}
        for tier in ('direct', 'group', 'residual', 'unplaced'):
            row[f'{tier}_%'] = 100 * float(by_tier.get(tier, 0.0)) / total
        row['placeable_%'] = row['direct_%'] + row['group_%']  # type: ignore[operator]
        row['commodities'] = int(fba.loc[fba['tier'] == 'direct', 'bea'].nunique())
        row['industries'] = int(fba['industry'].nunique())
        row['materials'] = int(fba['material'].nunique())
        row['suppressed_cells'] = int(fba['Suppressed'].notna().sum())
        if 'SuppressionRecovery' in fba:
            filled = fba['SuppressionRecovery'].notna() & fba['SuppressionRecovery'].ne(
                'unrecoverable'
            )
            row['recovered_$B'] = float(fba.loc[filled, 'FlowAmount'].sum()) / BILLION
        records.append(row)
    return pd.DataFrame(records).set_index('year')


def _matrix(
    fba: pd.DataFrame, materials_: list[str], industries: list[str]
) -> pd.DataFrame:
    return (
        fba.pivot_table(
            index='material', columns='industry', values='FlowAmount', aggfunc='sum'
        )
        .reindex(index=materials_, columns=industries)
        .fillna(0.0)
    )


def _shares(block: pd.DataFrame) -> pd.DataFrame:
    total = block.sum(axis=0)
    return block.div(total.where(total != 0, np.nan), axis=1).fillna(0.0)


def _shared_frame(
    recover: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, dict[str, float]]:
    early = materials(VINTAGES[0], recover=recover)
    late = materials(VINTAGES[1], recover=recover)
    industries = sorted(set(early['industry']) & set(late['industry']))
    codes = sorted(set(early['material']) & set(late['material']))
    covered = {
        f'{year}_cost_on_shared_frame_%': 100
        * float(
            frame[frame['industry'].isin(industries) & frame['material'].isin(codes)][
                'FlowAmount'
            ].sum()
        )
        / float(frame['FlowAmount'].sum())
        for year, frame in zip(VINTAGES, (early, late), strict=True)
    }
    a, b = _matrix(early, codes, industries), _matrix(late, codes, industries)
    return a, b, b.sum(axis=0), covered


def movement(recover: bool = True) -> pd.DataFrame:
    """Index of dissimilarity between the two vintages' materials mixes.

    Same metric as :mod:`~.intermediate_structure_drift`: the share of an
    industry's materials dollars sitting on the wrong material, dollar-weighted
    across industries with the column total given.
    """
    early, late, weights, covered = _shared_frame(recover=recover)
    per_column = (_shares(early) - _shares(late)).abs().sum(axis=0) / 2.0
    score = float((per_column * weights).sum() / weights.sum())
    row: dict[str, object] = {
        'industries': len(weights),
        'materials': early.shape[0],
        'dissimilarity': score,
        'median_column': float(per_column.median()),
        'columns_over_0.10': int((per_column > 0.10).sum()),
        'columns_over_0.25': int((per_column > 0.25).sum()),
        'columns_over_0.50': int((per_column > 0.50).sum()),
    }
    row.update(covered)
    return pd.DataFrame([row]).T.rename(columns={0: f'{VINTAGES[0]} -> {VINTAGES[1]}'})


def clean_movement() -> pd.DataFrame:
    """The mix score restricted to industries with nothing withheld in either year.

    ⚠️ **This is the number to quote, and the unrestricted one is the number to
    caveat.**  A recovered cell places the industry's residual across its
    withheld materials at a measured WAPE of 0.60 (``--holdout``), so for a
    column with heavy suppression the recovery, not the economy, decides the
    mix.  Restricting to columns where nothing was withheld removes that
    entirely -- at the cost of a smaller and not-quite-random sample, since
    suppression correlates with having few establishments.
    """
    early = materials(VINTAGES[0], recover=False)
    late = materials(VINTAGES[1], recover=False)
    withheld = set(early.loc[early['Suppressed'].notna(), 'industry']) | set(
        late.loc[late['Suppressed'].notna(), 'industry']
    )
    industries = sorted((set(early['industry']) & set(late['industry'])) - withheld)
    codes = sorted(set(early['material']) & set(late['material']))
    a, b = _matrix(early, codes, industries), _matrix(late, codes, industries)
    weights = b.sum(axis=0)
    per_column = (_shares(a) - _shares(b)).abs().sum(axis=0) / 2.0
    all_weights = late.groupby('industry')['FlowAmount'].sum()
    return pd.DataFrame(
        [
            {
                'industries': len(industries),
                'share_of_2022_cost_%': 100
                * float(weights.sum())
                / float(all_weights.sum()),
                'dissimilarity': float((per_column * weights).sum() / weights.sum()),
                'median_column': float(per_column.median()),
                'columns_over_0.10': int((per_column > 0.10).sum()),
                'columns_over_0.25': int((per_column > 0.25).sum()),
            }
        ]
    ).T.rename(columns={0: f'{VINTAGES[0]} -> {VINTAGES[1]}, unsuppressed only'})


def holdout(n_per_industry: int = 2, seed: int = 0) -> pd.DataFrame:
    """Mask published cells, recover them, and score against the truth.

    The only way to know whether the suppression prior is any good, and the
    measurement behind :data:`~bedrock.extract.census.Census_EC.NAICS_PEER_GROUP_LENGTH`.
    Every candidate peer-prefix length is scored; ``0`` is an economy-wide prior
    and ``6`` degenerates to it, since no other industry shares a 6-digit code.

    Recovered **mass** is always exact -- the residual is fixed by the published
    industry total -- so the error reported is purely allocation across
    materials.
    """
    rng = np.random.default_rng(seed)
    original = Census_EC.NAICS_PEER_GROUP_LENGTH
    records = []
    try:
        for year in VINTAGES:
            fba = getFlowByActivity('Census_EC_MatFuel', year)
            detail = fba[~fba['ActivityProducedBy'].isin(MATFUEL_TOTAL_CODES)]
            per_industry = detail.groupby('ActivityConsumedBy')['Suppressed'].apply(
                lambda s: s.notna().sum() == 0
            )
            unsuppressed = set(per_industry[per_industry].index)
            chosen: list[int] = []
            for _, group in detail[
                detail['ActivityConsumedBy'].isin(unsuppressed)
            ].groupby('ActivityConsumedBy'):
                usable = group[group['FlowAmount'] > 0]
                if len(usable) <= n_per_industry + 1:
                    continue
                chosen.extend(
                    rng.choice(usable.index, size=n_per_industry, replace=False)
                )
            truth = fba.loc[
                chosen, ['ActivityConsumedBy', 'ActivityProducedBy', 'FlowAmount']
            ].set_index(['ActivityConsumedBy', 'ActivityProducedBy'])['FlowAmount']
            masked = fba.copy()
            masked.loc[chosen, 'Suppressed'] = 'D'
            masked.loc[chosen, 'FlowAmount'] = 0.0

            for length in (0, 2, 3, 4, 5, 6):
                Census_EC.NAICS_PEER_GROUP_LENGTH = length
                recovered = estimate_suppressed_ec_matfuel(masked).set_index(
                    ['ActivityConsumedBy', 'ActivityProducedBy']
                )['FlowAmount']
                paired = pd.DataFrame(
                    {'true': truth, 'got': recovered.reindex(truth.index)}
                ).dropna()
                error = (paired['got'] - paired['true']).abs()
                records.append(
                    {
                        'year': year,
                        'peer_prefix': (
                            'economy-wide' if length == 0 else f'NAICS-{length}'
                        ),
                        'cells': len(paired),
                        'masked_$B': float(paired['true'].sum()) / BILLION,
                        'WAPE': float(error.sum() / paired['true'].sum()),
                        'median_rel_err': float(
                            (error / paired['true'].replace(0, np.nan)).median()
                        ),
                    }
                )
    finally:
        Census_EC.NAICS_PEER_GROUP_LENGTH = original
    return pd.DataFrame(records).set_index(['year', 'peer_prefix'])


def where(top: int = 15) -> pd.DataFrame:
    """Which industries moved, by dollars of materials spend reallocated."""
    early, late, weights, _ = _shared_frame()
    per_column = (_shares(early) - _shares(late)).abs().sum(axis=0) / 2.0
    # No industry label here: the FBA's Description column carries the *material*
    # name, and Census publishes the industry title only alongside it.
    table = pd.DataFrame(
        {
            'dissimilarity': per_column,
            f'materials_{VINTAGES[1]}_$B': weights / BILLION,
            'reallocated_$B': per_column * weights / BILLION,
        }
    )
    return table.sort_values('reallocated_$B', ascending=False).head(top)


@functools.cache
def _naics_to_bea_groups() -> dict[str, tuple[str, ...]]:
    """Ambiguous ``NAICS -> (every BEA detail commodity it covers)``.

    The companion to :func:`_naics_to_bea`, which keeps only the unambiguous
    side.  A ``group``-tier material is placeable precisely because this set is
    known and usually small.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    members = crosswalk.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].apply(
        lambda codes: tuple(sorted(set(codes)))
    )
    return {str(naics): bea for naics, bea in members.items() if len(bea) > 1}


def group_members(material: str) -> tuple[str, ...]:
    """The BEA detail commodities a ``group``-tier material could be.

    Empty for any other tier.  The longest NAICS prefix decides, exactly as in
    :func:`classify`, so the two always agree on which tier a material is in.
    """
    unique, _ = _naics_to_bea()
    groups = _naics_to_bea_groups()
    for length in (6, 5, 4, 3, 2):
        prefix = material[:length]
        if prefix in unique:
            return ()
        if prefix in groups:
            return groups[prefix]
    return ()


def _coarsened_group(material: str) -> tuple[str, ...]:
    """For a ``direct`` material, the group it falls into one prefix higher up.

    This is the holdout's masking rule: a material Census placed precisely is
    demoted to the group a less specific code would have reached, split with the
    prior, and scored against the placement that was thrown away.
    """
    unique, _ = _naics_to_bea()
    groups = _naics_to_bea_groups()
    for length in (6, 5, 4, 3, 2):
        prefix = material[:length]
        if prefix in unique:
            for shorter in range(length - 1, 1, -1):
                if material[:shorter] in groups:
                    return groups[material[:shorter]]
            return ()
        if prefix in groups:
            return ()
    return ()


@functools.cache
def _naics_to_bea_industry() -> dict[str, str]:
    """``NAICS -> BEA detail industry`` on the purchasing side of the table.

    The same crosswalk read down the other axis.  Several MATFUEL industries can
    land on one BEA detail industry, which is fine -- this only ever chooses a
    *column of the 2017 Use table* to read split weights from.
    """
    crosswalk = pd.read_csv(NAICS_TO_BEA, dtype=str).dropna(
        subset=['NAICS_2017_Code', 'BEA_2017_Detail_Code']
    )
    per_naics = crosswalk.groupby('NAICS_2017_Code')['BEA_2017_Detail_Code'].nunique()
    unique = crosswalk[
        crosswalk['NAICS_2017_Code'].isin(per_naics[per_naics == 1].index)
    ]
    return (
        unique.drop_duplicates('NAICS_2017_Code')
        .set_index('NAICS_2017_Code')['BEA_2017_Detail_Code']
        .to_dict()
    )


def bea_industry(naics: str) -> str | None:
    """The BEA detail industry column a MATFUEL NAICS-6 industry buys in.

    ✅ **All 388 (2017) and 367 (2022) MATFUEL industries resolve**, carrying
    100% of each vintage's cost, so the column prior below is never unavailable
    for want of an industry mapping.
    """
    industries = _naics_to_bea_industry()
    for length in (6, 5, 4, 3, 2):
        prefix = naics[:length]
        if prefix in industries:
            return industries[prefix]
    return None


@functools.cache
def _use_2017_detail() -> pd.DataFrame:
    """2017 benchmark detail Use intermediate block, commodity x industry, $M."""
    from bedrock.analysis.nowcasting.intermediate_structure_drift import (  # noqa: PLC0415
        benchmark_detail_intermediate,
    )

    return benchmark_detail_intermediate(2017)


#: How a ``group``-tier cell is divided among the BEA commodities it could be.
#: ``column`` reads the purchasing industry's own 2017 Use row for those
#: commodities; ``economy`` reads their economy-wide row totals.  ``column``
#: wins by a wide margin -- see :func:`group_split_holdout`.
GROUP_SPLIT_PRIORS = ('column', 'economy')


def _split_weights(
    members: tuple[str, ...], industry: str | None, prior: str
) -> pd.Series | None:
    """Normalised 2017 Use weights over a group's BEA commodities."""
    use = _use_2017_detail()
    present = [code for code in members if code in use.index]
    if not present:
        return None
    if prior == 'column' and industry in use.columns:
        column = use.loc[present, industry].clip(lower=0)
        if column.sum() > 0:
            return column / column.sum()
    # Falling back to the economy-wide row totals rather than to an equal split:
    # an industry buying nothing from a group in 2017 says the column is
    # uninformative, not that the group's commodities are interchangeable.
    rows = use.sum(axis=1).reindex(present).clip(lower=0)
    total = float(rows.sum())
    return rows / total if total > 0 else None


def place_on_commodities(
    year: int, prior: str = 'column', recover: bool = True
) -> pd.DataFrame:
    """MATFUEL cost placed on BEA detail commodities: ``direct`` plus ``group``.

    Long ``industry, bea, tier, FlowAmount``.  ``direct`` cells pass through;
    each ``group`` cell is divided over :func:`group_members` on 2017 Use shares;
    ``residual`` cells are dropped, because they are the part of the bill Census
    could not place at all.

    ⚠️ **The split is a 2017 structure used in both vintages**, so within a group
    it manufactures no movement -- only the group's total moves.  That is a real
    benchmark dependency and it damps :func:`commodity_movement` slightly.  It is
    still far lighter than freezing the whole column, which is the alternative:
    the group tier is 13.6% and 15.1% of cost, and the ~54% that is ``direct``
    moves freely.
    """
    fba = materials(year, recover=recover)
    direct = fba[fba['tier'] == 'direct']
    placed = [direct[['industry', 'bea', 'tier', 'FlowAmount']].copy()]

    group = fba[fba['tier'] == 'group']
    rows: list[dict[str, object]] = []
    for material, industry, amount in zip(
        group['material'].astype(str),
        group['industry'].astype(str),
        group['FlowAmount'].astype(float),
        strict=True,
    ):
        weights = _split_weights(group_members(material), bea_industry(industry), prior)
        if weights is None:
            continue
        for code, weight in weights.items():
            rows.append(
                {
                    'industry': industry,
                    'bea': str(code),
                    'tier': 'group',
                    'FlowAmount': amount * float(weight),
                }
            )
    if rows:
        placed.append(pd.DataFrame(rows))
    return (
        pd.concat(placed, ignore_index=True)
        .groupby(['industry', 'bea', 'tier'])['FlowAmount']
        .sum()
        .reset_index()
    )


def group_split_holdout() -> pd.DataFrame:
    """Score the within-group split against placements Census actually made.

    Every ``direct`` cell is demoted to the group one prefix higher
    (:func:`_coarsened_group`), split with the prior, and compared against the
    single commodity it really sat on.  The same shape of test as
    :func:`holdout`, and it is what chooses between :data:`GROUP_SPLIT_PRIORS`
    rather than an argument about which sounds better.

    ``on_right_commodity_%`` is the readable form: for a cell whose truth is one
    commodity ``WAPE = 2 * (1 - w_true)``, so half the WAPE subtracted from one
    is **the share of the split landing where it belongs**.

    ✅ **The column prior puts ~72% of the money on the right commodity against
    ~47% for an economy-wide one.**

    ⚠️ **Accuracy falls off with group breadth** -- about 80% for groups of 2-4
    commodities, 69% for 5-9, 52% for 10-29.  Roughly 73% of real group-tier
    dollars sit in groups of nine or fewer; the broad ``33`` prefix, 136
    commodities and $29.6B / $54.3B, is the weak end.
    """
    records = []
    for year in VINTAGES:
        fba = materials(year)
        direct = fba[fba['tier'] == 'direct'].copy()
        direct['members'] = [_coarsened_group(code) for code in direct['material']]
        direct = direct[direct['members'].map(len) > 0].copy()
        direct['industry_bea'] = [bea_industry(code) for code in direct['industry']]
        direct['bucket'] = pd.cut(
            direct['members'].map(len),
            [1, 4, 9, 29, 10_000],
            labels=['2-4', '5-9', '10-29', '30+'],
        ).astype(str)
        for prior in GROUP_SPLIT_PRIORS:
            errors: dict[str, float] = {}
            totals: dict[str, float] = {}
            for members, industry, bea, bucket, amount in zip(
                direct['members'],
                direct['industry_bea'],
                direct['bea'].astype(str),
                direct['bucket'].astype(str),
                direct['FlowAmount'].astype(float),
                strict=True,
            ):
                weights = _split_weights(members, industry, prior)
                if weights is None:
                    continue
                truth = pd.Series(0.0, index=weights.index)
                if bea in truth.index:
                    truth[bea] = amount
                error = float((weights * amount - truth).abs().sum())
                for key in ('all', bucket):
                    errors[key] = errors.get(key, 0.0) + error
                    totals[key] = totals.get(key, 0.0) + amount
            for key, error in errors.items():
                wape = error / totals[key]
                records.append(
                    {
                        'year': year,
                        'prior': prior,
                        'group_width': key,
                        '$B': totals[key] / BILLION,
                        'WAPE': wape,
                        'on_right_commodity_%': 100 * (1 - wape / 2),
                    }
                )
    return pd.DataFrame(records).set_index(['year', 'prior', 'group_width'])


@functools.cache
def _common_industry_basis() -> dict[tuple[str, str], str]:
    """``(vintage, NAICS) -> shared industry unit`` across the 2017/2022 revision.

    Connected components of the NAICS year concordance read as a bipartite
    graph.  A component is the finest industry both vintages can express: where
    2022 merged ``336111`` and ``336112`` into ``336110``, the component holds
    all three and the 2017 pair is summed to meet it.

    ✅ **Nothing is lost.**  Every MATFUEL industry in both vintages joins a
    component, and the 365 shared components carry **100% of each year's cost**
    against 89.8% and 90.7% on raw code equality.  Aggregating suffices because
    the revision is a pure merge in every case that carries money -- no 2017
    code needs splitting.
    """
    concordance = pd.read_csv(NAICS_YEAR_CONCORDANCE, dtype=str)[
        ['NAICS_2017_Code', 'NAICS_2022_Code']
    ].dropna()
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(node: tuple[str, str]) -> tuple[str, str]:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for early, late in concordance.drop_duplicates().itertuples(index=False):
        left, right = find(('2017', early)), find(('2022', late))
        if left != right:
            parent[left] = right
    return {node: '/'.join(find(node)) for node in list(parent)}


def _unit(vintage: int, naics: str) -> str:
    """The shared industry unit a vintage's NAICS code belongs to."""
    return _common_industry_basis().get((str(vintage), naics), f'{vintage}/{naics}')


def vintage_diff() -> pd.DataFrame:
    """What the 2017 -> 2022 code revision costs, before and after reconciling.

    ✅ **The material axis is not the problem.**  The two vintages share **289 of
    289 and 290** MATFUEL codes, and the single code unique to either year
    carries $0.1B.  Every warning in this repo about the vintages sitting on
    different bases is really a warning about the *industry* axis.

    ✅ **And the industry axis reconciles completely.**  All of the ~10%
    off-frame cost is NAICS 2022 merging pairs of 2017 codes -- ``336111`` +
    ``336112`` -> ``336110`` is the bulk of it, with 17 more behind it.  Summing
    the 2017 side to the merged unit recovers **100%** of both years, with no
    split assumption anywhere.

    ⚠️ **This does not rescue ``336411``.**  Aircraft manufacturing is on the
    shared frame in both vintages and always was; its 0.592 score is the
    suppression fill, not code churn, which is why §How far the materials mix
    actually moved excludes it rather than reconciling it.
    """
    early, late = materials(VINTAGES[0]), materials(VINTAGES[1])
    records = []
    for year, frame, other, other_year in (
        (VINTAGES[0], early, late, VINTAGES[1]),
        (VINTAGES[1], late, early, VINTAGES[0]),
    ):
        total = float(frame['FlowAmount'].sum())
        other_units = {_unit(other_year, code) for code in other['industry']}
        on_units = pd.Series(
            [_unit(year, code) in other_units for code in frame['industry']],
            index=frame.index,
        )
        records.append(
            {
                'year': year,
                'cost_$B': total / BILLION,
                'industries': frame['industry'].nunique(),
                'materials': frame['material'].nunique(),
                'shared_materials_%': 100
                * float(
                    frame.loc[
                        frame['material'].isin(set(other['material'])), 'FlowAmount'
                    ].sum()
                )
                / total,
                'shared_industry_codes_%': 100
                * float(
                    frame.loc[
                        frame['industry'].isin(set(other['industry'])), 'FlowAmount'
                    ].sum()
                )
                / total,
                'shared_industry_units_%': 100
                * float(frame.loc[on_units, 'FlowAmount'].sum())
                / total,
                'shared_units': len(
                    {_unit(year, code) for code in frame['industry']} & other_units
                ),
            }
        )
    return pd.DataFrame(records).set_index('year')


def merged_industries(top: int = 10) -> pd.DataFrame:
    """The 2022 NAICS merges that put the two vintages on different frames."""
    early, late = materials(VINTAGES[0]), materials(VINTAGES[1])
    early = early.assign(unit=[_unit(VINTAGES[0], code) for code in early['industry']])
    late = late.assign(unit=[_unit(VINTAGES[1], code) for code in late['industry']])
    rows = []
    for unit, block in early.groupby('unit'):
        codes = sorted(set(block['industry']))
        if len(codes) < 2:
            continue
        successor = late[late['unit'] == unit]
        rows.append(
            {
                'naics_2017': ' + '.join(codes),
                'naics_2022': ' + '.join(sorted(set(successor['industry']))),
                f'{VINTAGES[0]}_$B': float(block['FlowAmount'].sum()) / BILLION,
                f'{VINTAGES[1]}_$B': float(successor['FlowAmount'].sum()) / BILLION,
            }
        )
    return (
        pd.DataFrame(rows)
        .sort_values(f'{VINTAGES[1]}_$B', ascending=False)
        .head(top)
        .reset_index(drop=True)
    )


def commodity_movement(prior: str = 'column') -> pd.DataFrame:
    """The mix score where Step 3 needs it: BEA commodity x reconciled industry.

    :func:`movement` scores MATFUEL codes against MATFUEL codes, which is the
    right test of *the source*.  This scores what the seed is made of -- BEA
    detail commodities, on the reconciled industry basis of :func:`vintage_diff`
    -- which is the right test of *the deliverable*.

    Reported three ways, because together they bound the answer:

    ``direct only``
        no benchmark dependency at all, and no within-group assumption.
    ``direct + group``
        the full placeable bill, group cells split on 2017 Use shares.
    ``unsuppressed, direct + group``
        the same, restricted to industries with nothing withheld in either
        vintage -- the figure to quote, for the reason :func:`clean_movement`
        gives.

    ⚠️ **Expect a lower number than** :func:`movement`, **and it is not a
    correction to it.**  Aggregating 289 materials onto 139 commodities nets off
    substitution *within* a commodity, and the group split holds 2017 structure
    fixed inside each group.  Both push the score down, and both are properties
    of the seed rather than of the economy.
    """
    withheld = set()
    for year in VINTAGES:
        raw = materials(year, recover=False)
        withheld |= {
            _unit(year, code) for code in raw.loc[raw['Suppressed'].notna(), 'industry']
        }

    blocks: dict[tuple[int, tuple[str, ...]], pd.DataFrame] = {}
    records = []
    for label, tiers, clean in (
        ('direct only', ('direct',), False),
        ('direct + group', ('direct', 'group'), False),
        ('unsuppressed, direct + group', ('direct', 'group'), True),
    ):
        for year in VINTAGES:
            if (year, tiers) not in blocks:
                placed = place_on_commodities(year, prior=prior)
                placed = placed[placed['tier'].isin(tiers)].copy()
                placed['unit'] = [_unit(year, code) for code in placed['industry']]
                blocks[year, tiers] = placed.pivot_table(
                    index='bea', columns='unit', values='FlowAmount', aggfunc='sum'
                )
        first, second = blocks[VINTAGES[0], tiers], blocks[VINTAGES[1], tiers]
        units = sorted(set(first.columns) & set(second.columns))
        if clean:
            units = [unit for unit in units if unit not in withheld]
        codes = sorted(set(first.index) | set(second.index))
        a = first.reindex(index=codes, columns=units).fillna(0.0)
        b = second.reindex(index=codes, columns=units).fillna(0.0)
        weights = b.sum(axis=0)
        per_column = (_shares(a) - _shares(b)).abs().sum(axis=0) / 2.0
        records.append(
            {
                'frame': label,
                'industries': len(units),
                'commodities': int((b.sum(axis=1) > 0).sum()),
                f'{VINTAGES[1]}_$B': float(weights.sum()) / BILLION,
                'dissimilarity': float((per_column * weights).sum() / weights.sum()),
                'median_column': float(per_column.median()),
                'columns_over_0.10': int((per_column > 0.10).sum()),
                'columns_over_0.25': int((per_column > 0.25).sum()),
            }
        )
    return pd.DataFrame(records).set_index('frame')


#: The scope match for the ``Census_EC_MatFuel`` universe, in each survey's own
#: names.  ⚠️ **Not the ``CSTMTOT`` / ``EXPS_CSTMTOT_DVAL`` control**, which also
#: carries purchased electricity, contract work and resales and runs ~18% higher
#: -- see :func:`annual_path`.
ASM_MATFUEL_SCOPE = ('CSTMPRT', 'CSTFU')
AIES_MATFUEL_SCOPE = ('EXPS_MAT_DVAL', 'EXPS_FUEL_VAL')

#: The annual surveys that observe manufacturing's materials bill between and
#: after the two censuses, and the years each answers for.  ASM stops at 2021,
#: AIES replaced it from 2023, and **nothing covers 2022** (it is the census).
#: The observed span therefore ends at 2023; extending it is #707.
ANNUAL_SOURCES = {
    'Census_ASM_Expenses': (2018, 2019, 2020, 2021),
    'Census_AIES_Expenses': (2023,),
}

#: ``ASM name -> AIES name`` for the concepts the two share.  ✅ Verified
#: additive against each survey's own published total; see
#: :data:`~bedrock.extract.census.Census_AIES.AIES_EXPENSE_FLOWS`.
ASM_TO_AIES = {
    'CSTMPRT': 'EXPS_MAT_DVAL',
    'CSTFU': 'EXPS_FUEL_VAL',
    'CSTELEC': 'EXPS_ELEC_VAL',
    'CSTCNT': 'EXPS_CONTRACT_VAL',
    'CSTRSL': 'EXPS_RESALE_VAL',
}


def _annual_expenses() -> pd.DataFrame:
    """ASM and AIES stacked into one panel, six-digit NAICS only, in dollars.

    ``FlowName`` is normalised to the **ASM** name throughout, because ASM
    supplies four of the five years; :data:`ASM_TO_AIES` is the crosswalk and
    every AIES name outside it is left as published.
    """
    to_asm = {aies: asm for asm, aies in ASM_TO_AIES.items()}
    frames = []
    for source, years in ANNUAL_SOURCES.items():
        for year in years:
            fba = getFlowByActivity(source, year)
            # ⚠️ '31-33' is five characters, and the 3-, 4- and 5-digit parents
            # each cover all of manufacturing; match digits first, then length.
            naics = fba['ActivityConsumedBy'].astype(str)
            six = fba[naics.str.match(r'^\d{6}$')].copy()
            six['Year'] = year
            six['source'] = source
            six['FlowName'] = six['FlowName'].map(lambda name: to_asm.get(name, name))
            six['FlowAmount'] = six['FlowAmount'] * 1_000.0  # Census publishes $000
            frames.append(six)
    return pd.concat(frames, ignore_index=True)


def annual_path(kinds: tuple[str, ...] = ASM_MATFUEL_SCOPE) -> pd.DataFrame:
    """Does linear interpolation between the two censuses track the real path?

    **No, and not by a little.**  ASM observes each industry's materials bill
    every year of the gap, and the aggregate path is V-shaped -- 3,053 / 2,966 /
    2,636 / 3,109 $B over 2018-2021, against census endpoints of 2,830 and
    3,772.  A straight line between two points five years apart cannot bend, so
    it **overstates 2020 by 28.8%** and misses per-industry with a WAPE of
    **0.308**.

    ✅ **So the level belongs to ASM, not to an interpolation.**  It is observed
    at NAICS-6 for every year 2018-2021, with AIES ``EXPS_MAT_DVAL`` the
    successor series from 2023.

    ⚠️ **The mix is still not observed annually**, and nothing here changes that.
    What ASM buys is the *level and the coarse partition* -- materials, fuels,
    electricity, contract work, resales and ten named purchased services.  The
    commodity mix inside the materials bill remains a two-point interpolation.

    ⚠️ **Match the scope.**  :data:`ASM_MATFUEL_SCOPE` is ``CSTMPRT + CSTFU``.
    Against ``CSTMTOT`` the 2017-census-to-2018-ASM step is a median 1.181 across
    industries, which reads as a wild one-year jump but is three extra expense
    categories; on the matched scope it is 1.063, a year of materials inflation.
    """
    annual = _annual_expenses()
    annual = annual[annual['FlowName'].isin(kinds)].copy()
    annual['unit'] = [
        _unit(2017, code) for code in annual['ActivityConsumedBy'].astype(str)
    ]
    panel = annual.groupby(['unit', 'Year'])['FlowAmount'].sum().unstack('Year')
    observed_years = {
        year: source for source, years in ANNUAL_SOURCES.items() for year in years
    }

    census = {}
    for year in VINTAGES:
        frame = materials(year)
        census[year] = (
            frame.assign(unit=[_unit(year, code) for code in frame['industry']])
            .groupby('unit')['FlowAmount']
            .sum()
        )
    units = sorted(
        set(census[VINTAGES[0]].index)
        & set(census[VINTAGES[1]].index)
        & set(panel.dropna(how='any').index)
    )
    early, late = census[VINTAGES[0]].reindex(units), census[VINTAGES[1]].reindex(units)

    records = []
    for year in range(VINTAGES[0], max(observed_years) + 1):
        fraction = (year - VINTAGES[0]) / (VINTAGES[1] - VINTAGES[0])
        # Past 2022 the same straight line is an extrapolation rather than an
        # interpolation, which is the weaker claim of the two and the one 2023
        # tests.
        linear = early + (late - early) * fraction
        if year in VINTAGES:
            observed = early if year == VINTAGES[0] else late
            source = 'Economic Census'
        elif year in observed_years:
            observed = panel[year].reindex(units)
            source = (
                observed_years[year].removeprefix('Census_').removesuffix('_Expenses')
            )
        else:
            continue
        usable = observed.notna() & (observed > 0)
        error = (linear[usable] - observed[usable]).abs()
        records.append(
            {
                'year': year,
                'observed_from': source,
                'linear_$B': float(linear.sum()) / BILLION,
                'observed_$B': float(observed[usable].sum()) / BILLION,
                'aggregate_gap_%': 100
                * float(linear[usable].sum() - observed[usable].sum())
                / float(observed[usable].sum()),
                'industry_WAPE': float(error.sum() / observed[usable].sum()),
                'median_abs_%': 100 * float((error / observed[usable]).median()),
            }
        )
    return pd.DataFrame(records).set_index('year')


def unobserved_years() -> tuple[int, ...]:
    """Years in 2018-2025 with no observation of the materials bill.

    ⚠️ **2024 and 2025**, and both are **outside the current estimation span**
    rather than gaps in it.  ASM ends at 2021, AIES 2024 is not published, and
    the census is quinquennial, so the observed panel runs 2017-2023 and the
    estimate stops there.  Extending it to 2024 is #707 and is Phase 2 work;
    2025 is out of scope.  This reports the fact so a caller can assert on it --
    :func:`nonmaterial_seed` raises rather than extrapolating.
    """
    observed = {year for years in ANNUAL_SOURCES.values() for year in years}
    observed |= set(VINTAGES)
    return tuple(year for year in range(2018, 2026) if year not in observed)


def annual_partition() -> pd.DataFrame:
    """The coarse partition of manufacturing's column that ASM *does* observe.

    Each expense kind by year.  This is what a two-point interpolation gets to
    stop guessing about -- and the shape of the answer is that the *level* swings
    while the shares barely do: the fuels share of the materials bill sits near
    1% throughout, so the mix inside the census universe is not where the annual
    surveys help.  Electricity is the exception worth naming, because it is a
    commodity (``221100``) sitting outside the census materials universe
    altogether.

    ⚠️ **The two surveys do not name the same services.**  ASM publishes ten
    ``PCH*`` cells and AIES thirteen ``EXPS_*`` ones, overlapping but not
    identical, so ``purchased_services_$B`` is comparable across the ASM years
    and steps at 2023 for a definitional reason as well as an economic one.
    ``named_service_cells`` says how many cells went into each row, so the step
    is visible rather than silent.
    """
    annual = _annual_expenses()
    controls = [*ASM_EXPENSE_CONTROLS, *ASM_NON_COMMODITY, *AIES_EXPENSE_CONTROLS]
    flows = annual[~annual['FlowName'].isin(controls)]
    table = (
        flows.groupby(['Year', 'FlowName'])['FlowAmount'].sum().unstack('FlowName')
        / BILLION
    )
    # ⚠️ Not a ``PCH*`` prefix test: that is ASM's naming and silently reports
    # zero services for the AIES year, whose cells are ``EXPS_*``. Anything that
    # is not one of the five shared cost concepts is a purchased service.
    services = [column for column in table.columns if column not in ASM_TO_AIES]
    materials_and_fuels = table[list(ASM_MATFUEL_SCOPE)].sum(axis=1)
    return pd.DataFrame(
        {
            'materials+fuels_$B': materials_and_fuels,
            'electricity_$B': table['CSTELEC'],
            'contract_work_$B': table['CSTCNT'],
            'resales_$B': table['CSTRSL'],
            'purchased_services_$B': table[services].sum(axis=1),
            'named_service_cells': table[services].notna().sum(axis=1),
            'fuels_%_of_materials': 100 * table['CSTFU'] / materials_and_fuels,
        }
    )


# ---------------------------------------------------------------------------
# S3b -- the named non-materials cells
# ---------------------------------------------------------------------------

#: The four sources that observe manufacturing's non-materials expense cells,
#: and the years each answers for.  ``Census_EC_Expenses`` supplies **2017**,
#: which is what makes an index possible: it is the base year the benchmark Use
#: table is built on.  ⚠️ **The observed span ends at 2023**, exactly as for the
#: materials bill -- see :func:`unobserved_years`.
EXPENSE_SOURCES = {
    'Census_EC_Expenses': (2017, 2022),
    'Census_ASM_Expenses': (2018, 2019, 2020, 2021),
    'Census_AIES_Expenses': (2023,),
}

#: ``ASM/EC name -> the AIES cells that make it up``.  ⚠️ **Not one-for-one.**
#: AIES splits ASM's single repair cell into machinery and building, so the two
#: are summed back to ASM's concept; and it publishes rental of buildings and of
#: machinery, which ASM and the census do not carry at all, so they have no
#: entry here and no 2017 base to index against.
SERVICE_TO_AIES = {
    'CSTELEC': ('EXPS_ELEC_VAL',),
    'PCHADVT': ('EXPS_ADVERT_VAL',),
    'PCHCMPQ': ('EXPS_COMPTR_OTHEQ_VAL',),
    'PCHDAPR': ('EXPS_DATAPROC_VAL',),
    'PCHPRTE': ('EXPS_PROFTECH_VAL',),
    'PCHRFUS': ('EXPS_REFUSE_VAL',),
    'PCHRPR': ('EXPS_MACH_REP_VAL', 'EXPS_BUILD_REP_VAL'),
    'PCHTEMP': ('EXPS_TEMPSTAF_VAL',),
}

#: ⚠️ **Published by ASM and the census, and *not* by AIES.**  Both variables
#: exist in the 2023 AIES table and both are zero in every one of its 883 rows,
#: which is an absence rather than an economy that stopped buying telephony and
#: software.  They are held at the 2022 census observation rather than read as
#: zero; :func:`expense_panel` marks the year ``held``.
NO_AIES_COUNTERPART = ('PCHCSVC', 'PCHEXSO')

#: Expense kinds that are **not a purchase of one commodity** and are therefore
#: excluded from the seed rather than mapped badly.  ``CSTCNT`` contract work is
#: manufacturing services bought from other manufacturers -- a real intermediate
#: purchase, but one whose commodity is the *buyer's own* industry rather than
#: any fixed row.  ``CSTRSL`` resales are goods bought and sold on untransformed,
#: which the Use table handles through trade margins rather than as an input.
#: ``PCHOEXP`` is the survey's own residual.  Together they are named so the gap
#: they leave is visible rather than silent.
NOT_A_COMMODITY_PURCHASE = ('CSTCNT', 'CSTRSL', 'PCHOEXP')

#: ``expense kind -> the BEA 2017 detail commodities it buys``.  A kind covering
#: several commodities keeps BEA's own within-group split and moves the group
#: together, which adds no assumption of its own.
#:
#: ⚠️ **These carry an index, never a level** -- see :func:`expense_scope`, which
#: measures how far each survey cell sits from BEA's row and is the reason.
EXPENSE_TO_BEA = {
    'CSTELEC': ('221100',),
    'PCHADVT': ('541800',),
    'PCHCSVC': ('517110', '517A00', '517210'),
    'PCHDAPR': ('518200',),
    'PCHEXSO': ('511200',),
    'PCHCMPQ': ('334111',),
    'PCHPRTE': (
        '541100',
        '541200',
        '541300',
        '541511',
        '541512',
        '541610',
        '5416A0',
        '541700',
        '5419A0',
    ),
    'PCHRFUS': ('562000',),
    'PCHRPR': ('811100', '811200', '811300', '811400'),
    'PCHTEMP': ('561300',),
}


#: NAICS sector prefixes that are manufacturing, on either code basis.
MANUFACTURING = ('31', '32', '33')


def _manufacturing_bea_industries() -> list[str]:
    """The BEA detail industry columns that are manufacturing."""
    use = _use_2017_detail()
    return [code for code in use.columns if str(code)[:2] in MANUFACTURING]


def expense_panel() -> pd.DataFrame:
    """The non-materials expense cells, four sources on one set of names.

    Long ``bea_industry, kind, year, source, FlowAmount, held``.  Census 2017 and
    2022, ASM 2018-2021 and AIES 2023, normalised to the **ASM/census** names --
    which the Economic Census already uses, so only AIES needs
    :data:`SERVICE_TO_AIES`.

    ⚠️ Aggregated to **BEA detail industry**, because that is the axis the Use
    table's columns are on; several NAICS-6 industries can land on one column.

    ⚠️ ``held`` marks a cell carried rather than observed -- only
    :data:`NO_AIES_COUNTERPART` in 2023.  Read it before quoting a 2023 total
    for those two kinds.
    """
    kinds = tuple(EXPENSE_TO_BEA)
    frames = []
    for source, years in EXPENSE_SOURCES.items():
        for year in years:
            fba = getFlowByActivity(source, year)
            naics = fba['ActivityConsumedBy'].astype(str)
            six = fba[naics.str.match(r'^\d{6}$')].copy()
            six = six[
                six['ActivityConsumedBy'].astype(str).str[:2].isin(('31', '32', '33'))
            ]
            if source == 'Census_AIES_Expenses':
                rename = {
                    aies: kind
                    for kind, cells in SERVICE_TO_AIES.items()
                    for aies in cells
                }
                six['FlowName'] = six['FlowName'].map(
                    lambda name: rename.get(name, name)
                )
            six = six[six['FlowName'].isin(kinds)]
            column = pd.Series(
                [bea_industry(code) for code in six['ActivityConsumedBy'].astype(str)],
                index=six.index,
                dtype=object,
            )
            # ⚠️ Census publishes Thousand USD throughout, ASM and AIES included.
            six = six.assign(
                FlowAmount=six['FlowAmount'].astype(float) * 1_000.0,
                year=year,
                source=source,
                bea_industry=column,
                kind=six['FlowName'],
            )
            frames.append(six.dropna(subset=['bea_industry']))

    panel = (
        pd.concat(frames, ignore_index=True)
        .groupby(['bea_industry', 'kind', 'year', 'source'], as_index=False)[
            'FlowAmount'
        ]
        .sum()
    )
    panel['held'] = False

    # ⚠️ AIES publishes no telephony and no expensed software, so 2023 would read
    # as a total collapse for both. Carry the 2022 census -- the last observation
    # on this definition -- and say so, rather than seeding a zero.
    carried = [
        panel[(panel['kind'] == kind) & (panel['year'] == 2022)].assign(
            year=2023, source='held from 2022', held=True
        )
        for kind in NO_AIES_COUNTERPART
    ]
    carried = [frame for frame in carried if not frame.empty]
    if carried:
        panel = pd.concat([panel, *carried], ignore_index=True)
    return pd.DataFrame(panel)


def expense_scope() -> pd.DataFrame:
    """How far each survey expense cell sits from BEA's own 2017 Use row.

    **This is the measurement that decides the form of the seed, and the answer
    is that the levels cannot be used.**  Both sides are 2017, and both are
    manufacturing's purchases, so a matching pair of definitions would give a
    ratio near one.  They do not: it runs from **0.42 to 8.6**.

    The disagreements are structural rather than noise:

    - **Expensed software and computer hardware** are operating expense to
      Census and mostly **investment** to BEA, so BEA's intermediate row is a
      fraction of the survey cell.
    - **Repair** is one Census question against four BEA rows, and Census's cell
      carries parts and materials that BEA books elsewhere.
    - **Professional and technical services** runs the other way -- one Census
      question against BEA's legal, accounting, engineering, consulting and R&D
      rows together.

    ✅ **Indexing cancels every one of them**, because a constant scope factor
    divides out of ``survey(t) / survey(2017)``.  Substituting the level would
    import all of them into the row control at once.  That is why
    :func:`nonmaterial_seed` moves BEA's cell instead of replacing it.
    """
    use = _use_2017_detail()
    row = use[_manufacturing_bea_industries()].sum(axis=1) / 1000.0  # $B

    panel = expense_panel()
    base = panel[panel['year'] == 2017].groupby('kind')['FlowAmount'].sum() / BILLION

    records = []
    for kind, codes in EXPENSE_TO_BEA.items():
        present = [code for code in codes if code in row.index]
        missing = [code for code in codes if code not in row.index]
        bea = float(row.reindex(present).sum())
        survey = float(base.get(kind, float('nan')))
        records.append(
            {
                'kind': kind,
                'BEA_commodities': '+'.join(present),
                'not_in_use_table': ','.join(missing) or '-',
                'census_2017_$B': survey,
                'BEA_2017_$B': bea,
                'survey/BEA': survey / bea if bea else float('nan'),
            }
        )
    return pd.DataFrame(records).set_index('kind').sort_values('survey/BEA')


def nonmaterial_seed(year: int) -> pd.DataFrame:
    """The S3b seed: BEA's 2017 non-materials cells, moved on the survey index.

    ``commodity x BEA detail industry`` in $M, on the same axes as the benchmark
    Use table, for the manufacturing columns only.

    **The form is an index, not a substitution.**  For an expense kind ``k``
    mapped to commodities ``C``::

        seed[c, i] = Use2017[c, i] * survey[i, k, year] / survey[i, k, 2017]

    so BEA's own level and BEA's own split across ``C`` are both preserved and
    the survey supplies only the movement.  :func:`expense_scope` is why: the
    two disagree about levels by factors of 0.42 to 8.6, and every one of those
    cancels in the ratio.

    ⚠️ **An industry with no usable 2017 base holds its benchmark value**, rather
    than being dropped or zeroed -- a missing denominator is an absence of
    information about movement, which is what holding the benchmark means.

    ⚠️ **The observed span ends at 2023** (:func:`unobserved_years`), so this
    raises for any later year rather than inventing one.  Extending it to 2024
    is #707; 2025 is out of scope.
    """
    observed = {y for years in EXPENSE_SOURCES.values() for y in years}
    if year not in observed:
        raise ValueError(
            f'{year} is not observed for the expense cells; observed years are '
            f'{sorted(observed)}. Extrapolating past 2023 is not decided.'
        )

    use = _use_2017_detail()
    man = _manufacturing_bea_industries()
    wide = expense_panel().pivot_table(
        index='bea_industry', columns=['kind', 'year'], values='FlowAmount'
    )

    seed = pd.DataFrame(0.0, index=use.index, columns=pd.Index(man))
    for kind, codes in EXPENSE_TO_BEA.items():
        rows = [code for code in codes if code in use.index]
        if not rows:
            continue
        if (kind, year) not in wide.columns or (kind, 2017) not in wide.columns:
            continue
        base = wide[(kind, 2017)]
        index = (wide[(kind, year)] / base.where(base > 0)).reindex(man)
        index = index.replace([np.inf, -np.inf], np.nan).fillna(1.0)
        seed.loc[rows, man] = use.loc[rows, man].mul(index, axis=1).to_numpy()
    return seed.loc[(seed != 0).any(axis=1)]


def nonmaterial_movement() -> pd.DataFrame:
    """What the index does to the non-materials block, against a frozen 2017.

    The size of the correction S3b buys, in one table, rather than inferred.
    ⚠️ ``PCHCSVC`` and ``PCHEXSO`` are held in 2023 (:data:`NO_AIES_COUNTERPART`),
    so that row moves slightly less than a fully observed one would.
    """
    use = _use_2017_detail()
    man = _manufacturing_bea_industries()
    rows = [
        code for codes in EXPENSE_TO_BEA.values() for code in codes if code in use.index
    ]
    frozen = float(use.loc[rows, man].to_numpy().sum()) / 1000.0

    records = []
    for year in sorted({y for years in EXPENSE_SOURCES.values() for y in years}):
        seeded = float(nonmaterial_seed(year).to_numpy().sum()) / 1000.0
        records.append(
            {
                'year': year,
                'frozen_2017_$B': frozen,
                'seeded_$B': seeded,
                'change_%': 100 * (seeded - frozen) / frozen,
            }
        )
    return pd.DataFrame(records).set_index('year')


# ---------------------------------------------------------------------------
# S3 -- the interpolation form, and the materials seed
# ---------------------------------------------------------------------------

#: The exponent grid the price carry is fitted on, matching
#: :data:`~bedrock.analysis.nowcasting.intermediate_structure_drift.THETA_GRID`
#: so the two fits are directly comparable.  ⚠️ It runs negative deliberately: a
#: grid floored at 0.0 cannot represent shares moving *against* their own price
#: and silently reports that case as 0.0.
THETA_GRID = (-1.0, -0.75, -0.5, -0.25, 0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5)

#: The interpolation forms tested in :func:`interior_form_holdout`.
#: ⚠️ **A "hold the base, snap at the end year" step is not among them**: in the
#: interior it *is* ``frozen``, cell for cell, so it is not a distinct estimator
#: and testing it would report the same number twice.  ``endpoint`` is the
#: distinct candidate -- adopt the newer observation immediately.
INTERPOLATION_FORMS = ('frozen', 'linear', 'geometric', 'endpoint')

#: The form S3 ships, chosen by :func:`interior_form_holdout` rather than
#: assumed.  Log-linear in shares: it respects the simplex, which linear does
#: not, and it wins the one clean out-of-sample test that exists.
SHIPPED_FORM = 'geometric'

#: ⚠️ Below this the geometric form is undefined, so those cells fall back to
#: the linear one.  BEA's detail Use table has 8 negative cells in 161,604
#: (0.00%, -$0.8B) and the census materials block has none, so this is a
#: correctness guard rather than a treatment that moves a number.
_POSITIVE = 1e-15


def interpolate_shares(
    base: pd.DataFrame, end: pd.DataFrame, t: float, form: str = SHIPPED_FORM
) -> pd.DataFrame:
    """Column shares ``t`` of the way from ``base`` to ``end``, renormalised.

    ``t = 0`` returns ``base`` and ``t = 1`` returns ``end`` for every form, so
    the forms differ only in the interior -- which is exactly what
    :func:`interior_form_holdout` scores and the only thing at stake.

    ``frozen``
        ignore ``end`` entirely; the θ = 0 comparison every measurement here is
        scored against.
    ``linear``
        ``(1-t)·base + t·end``.
    ``geometric``
        ``base^(1-t) · end^t``, renormalised -- log-linear in shares.
    ``endpoint``
        take ``end`` at every ``t`` -- adopt the newer observation immediately.
        ⚠️ Not the same as a step that holds ``base`` and snaps at the end year;
        that step is ``frozen`` everywhere it differs from ``end``.

    ⚠️ **A non-positive cell has no geometric mean**, so those cells take the
    linear value instead; see :data:`_POSITIVE`.
    """
    if form not in INTERPOLATION_FORMS:
        raise ValueError(
            f'unknown form {form!r}; expected one of {INTERPOLATION_FORMS}'
        )
    if form == 'frozen':
        return base
    if form == 'endpoint':
        return end
    linear = base + t * (end - base)
    if form == 'linear':
        return linear
    usable = (base > _POSITIVE) & (end > _POSITIVE)
    geometric = (base.where(usable, 1.0) ** (1 - t)) * (end.where(usable, 1.0) ** t)
    geometric = geometric.where(usable, linear)
    total = geometric.sum(axis=0)
    return geometric.div(total.where(total != 0, np.nan), axis=1).fillna(0.0)


def materials_theta() -> pd.DataFrame:
    """❌ Fit the price carry on the census materials mix -- and reject it.

    The plan named a price-carried path as S3's candidate interpolation form,
    on the reasoning that #497 already carries a commodity price index and the
    observed *level* path is dominated by price.  This is that hypothesis tested
    on the one span that can score it: carry the 2017 census commodity mix to
    2022 on ``(p_c(2022)/p_c(2017)) ** theta`` and score against the observed
    2022 census mix.  The same move :mod:`~.intermediate_structure_drift`'s
    ``--theta`` makes on the summary panel, on this module's frame.

    ⚠️ **It fits θ = 0.00 on the unsuppressed subsample** -- the frame
    :func:`clean_movement` says to quote -- so **price explains none of the
    materials mix movement** and the price-carried path is not justified.  The
    full frame's θ = −0.25 is worth +0.4%, which is noise at this frame's
    resolution.  The level moves with price; the mix does not.
    """
    factor = commodity_price_factor(VINTAGES[1], base=VINTAGES[0])
    records = []
    for label, first, second, weights in _census_mix_frames():
        codes = list(first.index)
        priced = factor.reindex(codes)
        target = _shares(second)
        for theta in THETA_GRID:
            carried = carry_shares(first, priced.fillna(1.0), theta)
            per_column = (carried - target).abs().sum(axis=0) / 2.0
            records.append(
                {
                    'frame': label,
                    'theta': theta,
                    'dissimilarity': float(
                        (per_column * weights).sum() / weights.sum()
                    ),
                }
            )
    table = pd.DataFrame(records)
    frozen = table[table['theta'] == 0.0].set_index('frame')['dissimilarity']
    table['gain_vs_frozen_%'] = (
        100
        * (table['frame'].map(frozen) - table['dissimilarity'])
        / table['frame'].map(frozen)
    )
    return table.pivot(index='theta', columns='frame')


@functools.cache
def _census_mix_frames() -> (
    tuple[tuple[str, pd.DataFrame, pd.DataFrame, pd.Series], ...]
):
    """The 2017 and 2022 census commodity blocks, aligned, on both frames.

    The same construction :func:`commodity_movement` scores, returned rather
    than scored so the form measurements can reuse it.
    """
    withheld = set()
    for year in VINTAGES:
        raw = materials(year, recover=False)
        withheld |= {
            _unit(year, code) for code in raw.loc[raw['Suppressed'].notna(), 'industry']
        }

    blocks = {}
    for year in VINTAGES:
        placed = place_on_commodities(year)
        placed = placed[placed['tier'].isin(('direct', 'group'))].copy()
        placed['unit'] = [_unit(year, code) for code in placed['industry']]
        blocks[year] = placed.pivot_table(
            index='bea', columns='unit', values='FlowAmount', aggfunc='sum'
        )

    first_raw, second_raw = blocks[VINTAGES[0]], blocks[VINTAGES[1]]
    frames = []
    for label, clean in (
        ('direct + group', False),
        ('unsuppressed, direct + group', True),
    ):
        units = sorted(set(first_raw.columns) & set(second_raw.columns))
        if clean:
            units = [unit for unit in units if unit not in withheld]
        codes = sorted(set(first_raw.index) | set(second_raw.index))
        first = first_raw.reindex(index=codes, columns=units).fillna(0.0)
        second = second_raw.reindex(index=codes, columns=units).fillna(0.0)
        frames.append((label, first, second, second.sum(axis=0)))
    return tuple(frames)


def interior_form_holdout() -> pd.DataFrame:
    """✅ Which interpolation form, scored out of sample on the benchmark panel.

    ⚠️ **The published summary panel cannot answer this, and the reason matters.**
    BEA's annual summary tables are the last benchmark carried forward on annual
    indicators -- its 2022 and 2023 tables are still 2017-benchmark carries,
    since BEA has not incorporated the 2022 Economic Census -- so every interior
    year of a summary span is *itself* an interpolation.  Scoring one
    interpolation against another measures agreement between two methods, not
    the shape of the thing.

    The **benchmark detail panel** can answer it, because 2007, 2012 and 2017 are
    three independent Economic-Census-anchored observations.  Interpolate
    2007 → 2017 and score at the observed 2012:

    ======================  =============  ===========
    form                    manufacturing  whole table
    ======================  =============  ===========
    frozen at 2007                 0.0889       0.1323
    linear                         0.0764       0.1190
    **geometric**                  **0.0710**   **0.1164**
    endpoint (the 2017 mix)        0.1216       0.1712
    ======================  =============  ===========

    ✅ **Interpolating beats freezing** -- geometric by **+20.1%** on
    manufacturing -- and ✅ **geometric beats linear by +7.1%**, which is why
    :data:`SHIPPED_FORM` is not the obvious choice.  ❌ **Adopting the newer
    observation immediately is the worst of the four**, materially worse than
    freezing: it puts a five-year move into the first year of the span.

    ⚠️ **One interior observation, on a ten-year span**, transferred to the
    census's five-year one.  It is one clean test rather than many, and it is
    the only clean test the data admits.
    """
    return _panel_forms(interior=True)


def extrapolation_holdout() -> pd.DataFrame:
    """❌ Past the last observation, hold the mix -- do not extend the trend.

    Step 3's target years run past the last census: 2022 is observed, and 2023
    and 2024 are not.  So the extrapolation question is separate from the
    interior one and needs its own test.  From 2007 and 2012, reach the observed
    2017 benchmark:

    ================================  =============  ===========
    form                              manufacturing  whole table
    ================================  =============  ===========
    **hold the 2012 mix**             **0.1232**     **0.1733**
    linear trend extended                  0.1569        0.2455
    geometric trend extended               0.1513        0.2546
    ================================  =============  ===========

    ❌ **Extending the trend one span past its last observation costs 27.4% on
    manufacturing and 41.7% on the whole table.**  A mix trend does not persist,
    and this is the asymmetry S3 ships on: **interpolate between observations,
    hold flat past the last one.**
    """
    return _panel_forms(interior=False)


def _panel_forms(interior: bool) -> pd.DataFrame:
    """Score the forms on the benchmark detail panel; see the two callers."""
    blocks = {
        year: benchmark_detail_intermediate(year) for year in BENCHMARK_PANEL_YEARS
    }
    shares = {year: column_shares(block) for year, block in blocks.items()}
    records = []
    scored: BENCHMARK_YEAR
    for label, columns in (
        (
            'manufacturing',
            [c for c in blocks[2012].columns if str(c)[:2] in MANUFACTURING],
        ),
        ('whole table', list(blocks[2012].columns)),
    ):
        if interior:
            base, end, scored = shares[2007][columns], shares[2017][columns], 2012
            candidates = {
                form: interpolate_shares(base, end, 0.5, form)
                for form in INTERPOLATION_FORMS
            }
        else:
            base, end, scored = shares[2007][columns], shares[2012][columns], 2017
            candidates = {'hold the last observation': end}
            for form in ('linear', 'geometric'):
                # one span on from ``end`` is t = 2 on the base -> end line
                candidates[f'{form} trend extended'] = interpolate_shares(
                    base, end, 2.0, form
                )
        weights = blocks[scored][columns].sum(axis=0)
        for form, estimate in candidates.items():
            records.append(
                {
                    'frame': label,
                    'form': form,
                    'dissimilarity': dissimilarity(
                        estimate, shares[scored][columns], weights
                    )[0],
                }
            )
    table = pd.DataFrame(records).pivot(index='form', columns='frame')
    table.columns = table.columns.droplevel(0)
    reference = 'frozen' if interior else 'hold the last observation'
    return table.assign(
        **{
            'vs_%s_%%'
            % reference.split()[0]: lambda frame: 100
            * (frame.loc[reference, 'manufacturing'] - frame['manufacturing'])
            / frame.loc[reference, 'manufacturing']
        }
    )


def census_mix_on_bea_industries(
    clean: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The two census vintages' commodity blocks, on the BEA industry axis.

    ⚠️ **Aggregate the dollars, then take shares -- never average the ratios.**
    Several census units can map to one BEA detail industry, and the mean of
    their index ratios is not the index of their combined block: it weights a
    $40M industry like a $40B one.  Summing first makes the weighting the
    dollars' own.

    ``clean=True`` restricts to industries with nothing withheld in either
    vintage.  ⚠️ That is the right frame for *scoring* (see
    :func:`clean_movement`) and the wrong one for the *seed*, which wants every
    industry it can reach; the recovered cells are why
    :func:`~bedrock.extract.census.Census_EC.estimate_suppressed_ec_matfuel`
    exists.
    """
    _, first, second, _ = _census_mix_frames()[1 if clean else 0]
    mapping = pd.Series({unit: _unit_to_bea(unit) for unit in first.columns}).dropna()
    keep = list(mapping.index)
    grouped = [block[keep].T.groupby(mapping).sum().T for block in (first, second)]
    return grouped[0], grouped[1]


def materials_seed(year: int, clean: bool = False) -> pd.DataFrame:
    """The S3 seed: BEA's 2017 materials cells, moved on the census mix.

    ``commodity x BEA detail industry`` in $M, on the same axes as the benchmark
    Use table and the manufacturing columns only -- the same shape
    :func:`nonmaterial_seed` returns, so the two compose.

    **The form is an index on the share, not a substitution of the level**, for
    the reason §The 2017 anchor gives: the census materials universe and BEA's
    commodity cells are different objects, so their levels disagree and only
    their *movement* transfers::

        seed[c, i] = Use2017[c, i] * census_mix[c, i, year] / census_mix[c, i, 2017]

    then renormalised to hold the column total, because Step 3 owns the level
    through ``GO - VAPRO`` and this step supplies only the shape.

    The mix comes from :func:`interpolate_shares` at :data:`SHIPPED_FORM`, and
    each year's treatment is decided by measurement rather than convention:

    * **2017 and 2022** -- the census observes the mix; no interpolation.
    * **2018-2021** -- interpolated, geometric, per :func:`interior_form_holdout`.
    * **2023 onward** -- ⚠️ **held at the 2022 mix**, per
      :func:`extrapolation_holdout`, which finds that extending the trend one
      span costs 27.4%.

    ⚠️ **A cell absent from either census vintage holds its benchmark value.**
    An absent cell is an absence of information about movement, which is what
    holding the benchmark means -- not a fall to zero.

    ⚠️ **This is the materials half only.**  It covers the 79.4% of
    manufacturing's column that :data:`~bedrock.extract.census.Census_EC` places;
    :func:`nonmaterial_seed` is the 6.4% beside it.
    """
    if year < VINTAGES[0]:
        raise ValueError(f'{year} is before the first census vintage {VINTAGES[0]}')
    first, second = census_mix_on_bea_industries(clean=clean)
    span = VINTAGES[1] - VINTAGES[0]
    t = min((year - VINTAGES[0]) / span, 1.0)
    base_mix = _shares(first)
    mix = interpolate_shares(base_mix, _shares(second), t)

    index = (mix / base_mix.where(base_mix > 0)).replace([np.inf, -np.inf], np.nan)
    use = _use_2017_detail()
    man = _manufacturing_bea_industries()
    seed = use[man] * index.reindex(index=use.index, columns=man).fillna(1.0)
    totals, base_totals = seed.sum(axis=0), use[man].sum(axis=0)
    seed = seed.div(totals.where(totals != 0, np.nan), axis=1).mul(base_totals, axis=1)
    seed = seed.fillna(0.0)
    return seed.loc[(seed != 0).any(axis=1)]


def _unit_to_bea(unit: str) -> str | None:
    """A reconciled census industry unit back to a BEA detail industry.

    ⚠️ **A unit is ``"{vintage}/{naics}"``**, not a bare NAICS code -- that is
    what :func:`_common_industry_basis` names its components -- so the prefix has
    to come off before the crosswalk sees it.  Reading the unit as a code maps
    nothing at all and produces a seed that silently does not move.
    """
    return bea_industry(unit.split('/')[-1])


def materials_seed_movement(clean: bool = False) -> pd.DataFrame:
    """What the seed does to the materials block, against a frozen 2017.

    The size of the correction S3 buys, per year, rather than inferred -- the
    counterpart of :func:`nonmaterial_movement` for the materials half.
    """
    use = _use_2017_detail()
    man = _manufacturing_bea_industries()
    frozen = column_shares(use[man])
    records = []
    for year in range(VINTAGES[0], 2025):
        seed = materials_seed(year, clean=clean)
        estimate = column_shares(seed.reindex(index=use.index, columns=man).fillna(0.0))
        weights = use[man].sum(axis=0)
        records.append(
            {
                'year': year,
                'moved_from_frozen': dissimilarity(estimate, frozen, weights)[0],
                'treatment': (
                    'census observed'
                    if year in VINTAGES
                    else 'held at 2022' if year > VINTAGES[1] else 'interpolated'
                ),
            }
        )
    return pd.DataFrame(records).set_index('year')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coverage', action='store_true', help='what can be placed')
    parser.add_argument('--movement', action='store_true', help='2017 vs 2022 mix')
    parser.add_argument('--where', action='store_true', help='which industries moved')
    parser.add_argument(
        '--groups', action='store_true', help='split the group tier onto commodities'
    )
    parser.add_argument(
        '--vintage', action='store_true', help='reconcile the 2017/2022 code bases'
    )
    parser.add_argument(
        '--annual', action='store_true', help='ASM against a linear interpolation'
    )
    parser.add_argument(
        '--recovery', action='store_true', help='what suppression recovery changed'
    )
    parser.add_argument(
        '--holdout', action='store_true', help='score the suppression prior'
    )
    parser.add_argument(
        '--services',
        action='store_true',
        help='the non-materials cells, and the seed they carry',
    )
    parser.add_argument(
        '--form',
        action='store_true',
        help='the interpolation form: the price fit, the interior and the tail',
    )
    parser.add_argument(
        '--seed', action='store_true', help='the built materials seed, per year'
    )
    parser.add_argument('--all', action='store_true', help='every measurement')
    args = parser.parse_args()
    chosen = (
        args.coverage
        or args.movement
        or args.where
        or args.recovery
        or args.holdout
        or args.groups
        or args.vintage
        or args.annual
        or args.services
    )
    pd.set_option('display.width', 200)

    if args.all or args.coverage or not chosen:
        print('\nHow much of the materials bill can be placed on a BEA commodity')
        print('(shares of cost after suppression recovery)\n')
        print(coverage().round(1).to_string())
    if args.all or args.movement:
        print('\nHow far the materials mix moved between the two censuses')
        print('(share of an industry of materials dollars on the wrong material)\n')
        print(movement().round(4).to_string())
        print()
        print(clean_movement().round(4).to_string())
    if args.all or args.where:
        print('\nWhere the movement sits\n')
        print(where().round(3).to_string())
    if args.all or args.holdout:
        print('\nScoring the suppression prior against masked truth')
        print('(recovered mass is exact by construction; this is allocation error)\n')
        print(holdout().round(3).to_string())
    if args.all or args.recovery:
        print('\nWhat suppression recovery changed\n')
        compare = pd.concat(
            {'withheld left at zero': coverage(recover=False), 'recovered': coverage()},
            names=['treatment'],
        )[['published_$B', 'direct_%', 'group_%', 'residual_%', 'placeable_%']]
        print(compare.round(2).to_string())
        print(
            '\n  mix score, withheld at zero: '
            f'{movement(recover=False).loc["dissimilarity"].iloc[0]:.4f}'
        )
        print(
            '  mix score, recovered:        '
            f'{movement().loc["dissimilarity"].iloc[0]:.4f}'
        )
    if args.all or args.groups:
        print('\nSplitting the group tier onto BEA commodities, on 2017 Use shares')
        print('(share of a split landing on the commodity Census actually named)\n')
        print(
            group_split_holdout()
            .reset_index()
            .pivot(
                index=['year', 'group_width'],
                columns='prior',
                values='on_right_commodity_%',
            )
            .round(1)
            .to_string()
        )
        print('\nWhat the split buys, on the BEA commodity frame Step 3 seeds\n')
        print(commodity_movement().round(4).to_string())
    if args.all or args.vintage:
        print('\nThe 2017 / 2022 code revision, before and after reconciling\n')
        print(vintage_diff().round(2).to_string())
        print('\nThe NAICS 2022 merges behind it\n')
        print(merged_industries().round(2).to_string(index=False))
    if args.all or args.annual:
        print('\nLinear interpolation against the observed annual path')
        print('(materials + fuels, the scope Census_EC_MatFuel covers)\n')
        print(annual_path().round(2).to_string())
        print('\nWhat ASM observes annually that the census does not\n')
        print(annual_partition().round(2).to_string())
    if args.all or args.form:
        print('\nDoes the price carry explain the materials mix?')
        print('(theta on the census span, scored at the observed 2022 mix)\n')
        print(materials_theta().round(4).to_string())
        print(
            '\n  theta = 0.00 on the frame to quote: the price carry buys'
            '\n  NOTHING on the mix. The level moves with price; the mix'
            '\n  does not, so the price-carried path is not justified.'
        )
        print('\nWhich interpolation form, scored out of sample on the')
        print('benchmark panel (2007 -> 2017, scored at the observed 2012)\n')
        print(interior_form_holdout().round(4).to_string())
        print(
            '\n  the published summary panel CANNOT score this: BEA carries'
            '\n  the last benchmark forward, so its interior years are'
            '\n  themselves an interpolation, and its 2022 and 2023 tables'
            '\n  are still 2017-benchmark carries.'
        )
        print('\nAnd past the last observation (2007, 2012 -> observed 2017)\n')
        print(extrapolation_holdout().round(4).to_string())
        print('\n  so: interpolate between observations, hold past the last.')
    if args.all or args.seed:
        print('\nWhat the materials seed does, against a frozen 2017\n')
        print(materials_seed_movement().round(4).to_string())
        print('\n  the same, on the unsuppressed frame only\n')
        print(materials_seed_movement(clean=True).round(4).to_string())
        built = materials_seed(VINTAGES[1])
        print(
            f'\n  {VINTAGES[1]} seed: {built.shape[0]} commodities x '
            f'{built.shape[1]} industries, ${built.sum().sum() / 1000:,.0f}B'
            '\n  the column total is held: the seed supplies shape, and'
            '\n  Step 3 owns the level through GO - VAPRO.'
        )
    if args.all or args.services:
        print('\nThe named non-materials cells: how far the survey sits from BEA')
        print('(both sides 2017, both manufacturing -- a scope match would be 1.0)\n')
        print(expense_scope().round(2).to_string())
        print(
            '\n  the spread is why the seed carries an index and not a level:'
            '\n  a constant scope factor cancels in survey(t)/survey(2017).\n'
        )
        print('What the index does to the block, against a frozen 2017\n')
        print(nonmaterial_movement().round(2).to_string())
        print(
            f'\n  unobserved, so absent above: {unobserved_years()}'
            '\n  PCHCSVC and PCHEXSO are held from 2022 in 2023 (no AIES cell).'
        )
    print()


if __name__ == '__main__':
    main()
