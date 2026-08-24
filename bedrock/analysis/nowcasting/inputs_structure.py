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
sources read here reach **91.0% of it**:

============================================ =======
``Census_EC_MatFuel`` materials                79.4%
named non-materials cells, ASM and AIES        11.7%
neither                                         9.0%
============================================ =======

The 11.7% is electricity (1.4%), resales (3.7%), contract work (1.5%) and nine
to twelve named purchased-service cells -- repair 1.6%, temp staffing 1.1%,
professional and technical 1.0%, advertising 0.5%, refuse 0.4%, data processing,
communication, expensed software and computers behind them.  Every one of those
maps onto a BEA service commodity.

⚠️ **The measurements below are still about the materials mix**, which is what
has been built.  The non-materials cells are so far only *reported*, by
:func:`annual_partition`, not turned into a seed.  That is the obvious next
extension and it is why the module is named for the column now rather than
after it.

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
   (2023).  It is not.

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
**0.0941** (:func:`commodity_movement`).  That is the number comparable to
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

⚠️ **2024 and 2025 are unobserved** (:func:`unobserved_years`).  ASM ends at
2021, AIES 2024 is unpublished, and the census is quinquennial, so the last two
years of the nowcast have nothing behind them at all.

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
    MATFUEL_TOTAL_CODES,
    estimate_suppressed_ec_matfuel,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

#: Same file :mod:`~.pxi_mix_test` reads, and by the same repo-relative path.
NAICS_TO_BEA = 'bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv'

#: The 2017 <-> 2022 NAICS concordance, which is what makes the two census
#: vintages comparable at all.  See :func:`_common_industry_basis`.
NAICS_YEAR_CONCORDANCE = 'bedrock/utils/mapping/naics/NAICS_Year_Concordance.csv'

#: The two Economic Census vintages either side of the nowcast span's midpoint.
VINTAGES = (2017, 2022)

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
    """``(tier, bea_detail_code)`` for one 8-digit ``MATFUEL`` code."""
    if material in MATFUEL_RESIDUAL_CODES:
        return 'residual', None
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
#: AIES replaced it from 2023, and **nothing covers 2022 (it is the census),
#: 2024 or 2025** -- so the end of the nowcast span is unobserved.
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
        # interpolation, which is the weaker claim of the two and the one the
        # nowcast actually leans on for 2023-2025.
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
    """Years of the nowcast span with no observation of the materials bill.

    ⚠️ **2024 and 2025.**  ASM ends at 2021, AIES 2024 is not published, and the
    census is quinquennial -- so the two years the nowcast most needs are the two
    with nothing behind them.  Whatever form the path takes, it is an
    extrapolation there, and :func:`annual_path` shows what extrapolating a
    straight line costs on the years that *can* be checked.
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
    print()


if __name__ == '__main__':
    main()
