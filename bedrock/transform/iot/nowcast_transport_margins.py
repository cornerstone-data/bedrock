"""Per-mode transport margin allocation for the nowcast Margins dataset.

Step 4c of the nowcast build
(``bedrock/analysis/nowcasting/margins_estimation_plan.md``), phase 2
(`#611 <https://github.com/cornerstone-data/bedrock/issues/611>`_). This module
distributes each transport mode's margin output across the commodities that
receive it, on the basis BEA actually uses for that mode.

**Modes are never combined.** *"We do not combine modes and we ignore
multi-modal reported data since we cannot differentiate those in margin
output."* - W. Nicolls, BEA, 2026-08-11. Each mode's margin is spread only over
freight moving by that mode, on its own basis, so this module is a per-mode
dispatch rather than one allocator:

===========  ======  =========================================  ==========
mode         share   basis                                      state
===========  ======  =========================================  ==========
**truck**    67.8%   revenue by commodity group, SAS Table 8    **built**
                     (AIES ``miscsector`` from 2023)
**rail**     16.5%   revenue by product, STB CRSR by STCC5      **built**
**pipeline** 11.9%   four Census margin items -> commodity sets **built**
**water**     2.3%   ton-miles x difficulty multiplier          **built**
**air**       1.5%   ton-miles x difficulty multiplier          **built**
===========  ======  =========================================  ==========

⚠️ **An earlier version of this module allocated every mode on FAF ton-miles.**
BEA's reply retired it and the PR was closed (`#627
<https://github.com/cornerstone-data/bedrock/pull/627>`_); volume is the basis
for water and air only, 3.8% of the column between them. Do not reintroduce a
single all-mode volume allocator.

Pipeline
--------

Built first because it is the only mode needing no external allocation source:

    *"Pipeline is a bit different from our other transportation margins in that
    there is no outside source that we need to tell us where to distribute the
    pipeline margins. The margin values, like the other transportation margins,
    come from Census and there are 4 pipeline margine items... These margins are
    all distributed proportionally to the commodities to which they are
    assigned."* - W. Nicolls, BEA, 2026-08-17

The four items are the four detailed pipeline NAICS, and they partition NAICS
486 to the dollar in every year: ``4861`` crude oil, ``4862`` natural gas,
``48691`` refined petroleum products, ``48699`` all other. Census publishes them
annually in **Service Annual Survey Table 2**, 2013-2022, and in **AIES**
``timeseries/aies/basic`` at ``TYPOP`` ``00`` from 2023. Each maps to a named
BEA 2017 commodity set in :data:`PIPELINE_CROSSWALK_PATH`, and within a set the
margin is split proportionally.

**Crude and natural gas collapse onto one commodity.** BEA 2017 detail carries a
single ``211000`` *Oil and gas extraction*, so ``4861`` and ``4862`` - 80.3% of
2017 pipeline revenue between them - land together and their split never affects
the allocation. Only a three-way split does any work.

**The bound test is what validates this.** The published ``Transportation``
column is summed over all five modes, so it is *not* an answer key for pipeline
alone - pipeline's share of any commodity can only be checked against a ceiling.
:func:`pipeline_bound_check` reports it. On 2017 the allocation fits everywhere
with plausible headroom: 83.8% of ``211000`` (pipeline dominates crude and gas
haulage), 34.3% of the refinery commodities, 17.7% of the NEC chemicals.

Rail
----

Allocated on **revenue by product shipped**, BEA's stated basis, taken from the
STB Commodity Revenue Stratification Report rather than the AAR data BEA buys -
see ``STB_CRSR``. 371 five-digit STCC codes map to 136 BEA commodities through
:data:`RAIL_CROSSWALK_PATH`.

Rail is better sourced than pipeline in two ways. Revenue is observed per
commodity, so it *is* the weight and there is no within-set default to defend.
And the source total reconciles: 68,926 $M all-data against a 68,598 $M
published give-up, 0.48% apart.

⚠️ **Fifteen codes are excluded, 16.3% of released revenue**, because they name
a service class or an empty move - trailer-on-flatcar and NEC rate shipments
(14.0% alone), freight-forwarder traffic, returned empties, small packaged
freight. They are dropped and the rest renormalised, which is what BEA does with
the equivalent bucket on the *truck* side; applying it to rail is an inference.

The two modes fit together, which is the stronger claim. Neither exceeds the
ceiling on any commodity, and what they leave - 296,321 $M - agrees within 0.33%
with the 297,310 $M that truck, water and air give up on their own rows. Those
are opposite sides of the table, so the agreement is evidence rather than an
identity.

Truck
-----

Allocated on SAS Table 8 revenue by commodity group, BEA's stated basis. Eleven
groups partition Total Motor Carrier Revenue to the dollar; ten are used and
**"Other goods" - 32.4% - is discarded**, because BEA does not use it and says
pro rata "would not change the result". The hazardous-materials row is a
cross-cut of the same revenue, not a group, and is excluded.

**AIES continues Table 8 from 2023** on ``timeseries/aies/miscsector``, whose
``RCPT_MOTR_<group>_DVAL`` items carry the *same eleven group names* - so
:data:`TRUCK_CROSSWALK_PATH` joins them unchanged, and
``test_aies_continues_the_sas_taxonomy_across_the_2023_seam`` fails if Census
ever renames one. ``Census_AIES_MiscSector`` emits them under Table 8's own
``FlowName`` strings, which is why everything below the source dispatch in
:func:`load_truck_group_revenue` is one implementation across the seam.

⚠️ **The shares step at the seam and it is not silent.** Mean |Δpp| across
2022->2023 is 1.39 against 0.34 within SAS, four times the normal volatility,
located exactly at the survey consolidation. It is carried as observed because
the three artifact tests pass - the taxonomy is unchanged, the eleven groups
still partition the published total, and the biggest movers ("Used household
and office goods" 6.69% -> 9.94%, "Coal and petroleum products" 6.99% -> 9.56%)
are consistent with the 2023 freight recession. This is the same verdict, on
the same evidence, that the retail rate step got on the trade side. The level
does not matter - it comes from the 2017 anchor - but the shares do.

⚠️ **Truck's commodity detail comes mostly from the weight, not the source.** Ten
groups span 258 commodities, so unlike rail - where revenue is observed per
commodity - most of the within-group split is inferred. BEA calls Table 8 *"a
very aggregated level"* and faces the same limit.

✅ Air's 2023 control total - closed on its own volume series
--------------------------------------------------------------

**``481212`` more than doubles across the AIES splice and it is not a real
move.** Nonscheduled chartered freight air runs 4,846 / 4,857 / 4,987 / 6,045 $M
over 2019-2022 in SAS Table 2 - unsuppressed - and AIES 2023 publishes
13,271 $M. With ``481112`` that puts air freight revenue at **2.32x** its 2017
level.

**FAF settles it.** Air ton-miles for the same mode are **1.06x** 2017 in 2023,
having fallen back from 1.32x in 2022, so the published revenue implies unit
revenue doubling in one year - in the year air cargo rates collapsed from their
pandemic peak. Wrong size and wrong sign:

=====  =========  =========  ==============
year   rev index  vol index  implied $/t-mi
=====  =========  =========  ==============
2021        1.18       1.16            1.02
2022        1.68       1.32            1.27
2023        2.32       1.06            **2.19**
=====  =========  =========  ==============

So from 2023 air's control moves on volume, holding unit revenue at its last
observed value - see :func:`air_revenue_from_volume`. 2023 air revenue becomes
14,386 $M rather than 24,734, unit revenue stays flat at 1.27, and air lands at
**1.6% of TRANS**, back at the share the table above states. Uncorrected it
would have been 2.7%.

⚠️ **The comparison that misleads is 2022 -> 2023 alone.** Measured from the 2017
anchor the two air codes grow 2.17x and 2.47x, which looks comparable and makes
the seam look benign. It is the volume series, not the revenue series, that
shows the break - which is why an independent source was needed to close this
rather than more arithmetic on the same numbers.

⚠️ **Water is not treated this way and must not be.** Its unit revenue moves -4%
across the same seam (1.65 -> 1.58). Only air breaks.

⚠️ The three built modes do not yet fit jointly
-----------------------------------------------

Each mode is right on its own total, and pipeline and rail each fit under the
ceiling. Adding truck breaks it: **95 of 258 commodities are over-allocated by
37,826 $M**, 9.1% of the column. Weighting truck on the room pipeline and rail
leave shrinks that to 15,332 $M but does not close it, because a group's share
of truck revenue can exceed the room left anywhere in that group - grain farming
is the worst case.

**Water and air made it worse, as expected.** They are only 3.8% of the column
but concentrate in commodities the other modes already claim, so all five
together over-allocate **97 of 258 commodities by 45,232 $M**, 10.9% of the
column - up from 37,826 with three. They filled no gaps.

The aggregate is not the problem: the five give-ups total 415,548 $M against a
published column of 414,559 $M, a 0.24% difference that is the known
give-up-versus-receiving gap. It is the distribution that collides, and the fix
is a **joint solve** - a mode x commodity
fit whose row totals are the five give-ups and whose column totals are the
published column, seeded with each mode's independent allocation so its own
evidence still shapes the answer. Sequential weighting cannot get there, because
no ordering makes the constraint hold in both directions at once.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.transform.iot.nowcast_margins import (
    COMMODITY_LEVEL,
    load_margins_transactions_2017,
)

# The survey consolidation is one fact for both margin sides, so the year lives
# in one place. The trade module holds no import of this one, so no cycle.
from bedrock.transform.iot.nowcast_trade_margins import FIRST_AIES_YEAR

#: The margin-item -> BEA 2017 commodity map, with the judgement calls recorded.
PIPELINE_CROSSWALK_PATH = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'Crosswalk_Pipeline_Margin_Items_to_BEA_2017.csv'
)

#: The four detailed pipeline NAICS BEA calls its "pipeline margin items".
PIPELINE_ITEM_CODES = ('4861', '4862', '48691', '48699')

#: The NAICS the four items partition. Used to check that they still do.
PIPELINE_TOTAL_CODE = '486'

#: BEA 2017 detail code for the pipeline commodity, whose own rows carry the
#: margin it gives up.
PIPELINE_COMMODITY = '486000'

TRANSPORT_COLUMN = 'Transportation'

MARGIN_COMPONENTS = ["Producers' Value", 'Transportation', 'Wholesale', 'Retail']


def load_pipeline_crosswalk() -> pd.DataFrame:
    """The margin item -> BEA 2017 commodity map. One row per (item, commodity)."""
    return pd.read_csv(PIPELINE_CROSSWALK_PATH, dtype=str)


def load_pipeline_item_revenue(year: int) -> pd.Series:
    """
    The four pipeline margin items' revenue for *year*, USD, from SAS Table 2.

    Raises if the four no longer partition NAICS 486, which is the check that
    the published taxonomy still matches the four items BEA named.
    """
    if year >= FIRST_AIES_YEAR:
        # AIES replaced the Service Annual Survey from data year 2023. The four
        # items and the NAICS 486 total are published on timeseries/aies/basic
        # at TYPOP 00, and still partition to the dollar - the check below is
        # what enforces that, and it is the same check either side of the seam.
        source = 'Census_AIES'
        fba = getFlowByActivity(source, year)
        table2 = fba[fba['FlowName'].astype(str).str.strip() == 'Sales']
    else:
        source = 'Census_SAS'
        fba = getFlowByActivity(source, year)
        table2 = fba[fba['Description'].astype(str).str.startswith('Table 2')]

    revenue = (
        table2[table2['ActivityProducedBy'].isin(PIPELINE_ITEM_CODES)]
        .groupby('ActivityProducedBy')['FlowAmount']
        .sum()
        .reindex(list(PIPELINE_ITEM_CODES))
    )
    if revenue.isna().any():
        missing = sorted(revenue.index[revenue.isna()])
        raise ValueError(
            f'{source} has no pipeline revenue for {missing} in {year}. '
            f'Those are BEA pipeline margin items; a missing one would silently '
            f'drop its commodity set from the allocation.'
        )

    published_total = table2[table2['ActivityProducedBy'] == PIPELINE_TOTAL_CODE][
        'FlowAmount'
    ].sum()
    if published_total and abs(revenue.sum() / published_total - 1) > 1e-6:
        raise ValueError(
            f'The four pipeline margin items sum to {revenue.sum():,.0f} against a '
            f'published NAICS 486 total of {published_total:,.0f} in {year}, from '
            f'{source}. They are meant to partition it exactly - a gap means the '
            f'detailed NAICS no longer match the four items BEA named.'
        )
    return revenue


def pipeline_margin_2017(margins: pd.DataFrame | None = None) -> float:
    """
    The margin ``486000`` gives up in 2017, USD, from the published table.

    This is the level being allocated. It is read off the pipeline commodity's
    own rows as ``sum over buyers of (components - Purchasers' Value)`` rather
    than from the Supply table, because the published ``Purchasers' Value`` on a
    transport commodity's rows is not the sum of its components - that margin
    has been moved onto the goods.
    """
    return _mode_give_up_2017(PIPELINE_COMMODITY, margins)


def _mode_give_up_2017(commodity: str, margins: pd.DataFrame | None = None) -> float:
    """The margin *commodity* gives up in 2017, USD, off its own rows."""
    df = load_margins_transactions_2017() if margins is None else margins
    rows = df.loc[df.index.get_level_values(COMMODITY_LEVEL) == commodity]
    return float(
        (rows[MARGIN_COMPONENTS].sum(axis=1) - rows["Purchasers' Value"]).sum()
    )


def published_transport_by_commodity(margins: pd.DataFrame | None = None) -> pd.Series:
    """Published ``Transportation`` per commodity, all five modes summed. USD."""
    df = load_margins_transactions_2017() if margins is None else margins
    return df.groupby(level=COMMODITY_LEVEL)[TRANSPORT_COLUMN].sum()


def pipeline_allocation(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
    within_set_weight: pd.Series | None = None,
) -> pd.Series:
    """
    Pipeline margin per BEA 2017 commodity for *year*. USD, indexed by commodity.

    Each margin item's share of pipeline revenue in *year* sets how much of
    *control_total* it carries; within its commodity set that amount is split
    proportionally on the published 2017 ``Transportation`` column.

    *within_set_weight* defaults to the published ``Transportation`` column.
    That inherits BEA's own commodity allocation for the same reason the whole
    anchor-and-move construction does, and it cannot push a commodity past the
    ceiling :func:`pipeline_bound_check` tests. Pass a Series to use another -
    commodity output ``T013`` and FAF pipeline-mode ton-miles are the obvious
    alternatives, and the second is the only one that is actually mode-specific.

    ⚠️ **The default weight assumes pipeline's mix within a set matches the
    all-mode mix**, which is the one place this construction leans on other
    modes' behaviour. It is bounded: 80.4% of pipeline margin goes to ``211000``,
    whose set holds a single commodity, so no weight touches it. Across the whole
    column the choice between the published-transport and ``T013`` weights moves
    at most 536 million, 1.1% of pipeline and 0.13% of ``TRANS`` - all of it
    between ``324110`` and ``324190``.

    *control_total* defaults to the 2017 published give-up, which makes 2017 an
    identity; a nowcast year passes its own.
    """
    if control_total is None:
        control_total = pipeline_margin_2017(margins)

    revenue = load_pipeline_item_revenue(year)
    shares = revenue / revenue.sum()
    published = published_transport_by_commodity(margins)
    basis = published if within_set_weight is None else within_set_weight
    crosswalk = load_pipeline_crosswalk()

    allocation: dict[str, float] = {}
    for item, group in crosswalk.groupby('sas_naics'):
        commodities = list(group['bea_2017_commodity'])
        weights = basis.reindex(commodities).fillna(0.0)
        if weights.sum() <= 0:
            raise ValueError(
                f'Pipeline margin item {item} maps to {commodities}, none of which '
                f'receives transportation margin in the published 2017 table, so '
                f'there is no basis on which to split its margin.'
            )
        item_margin = float(shares[item]) * control_total
        for commodity, share in (weights / weights.sum()).items():
            allocation[commodity] = allocation.get(commodity, 0.0) + item_margin * share

    return pd.Series(allocation, name='pipeline').sort_values(ascending=False)


def pipeline_bound_check(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Pipeline's allocation against the published all-mode ceiling, per commodity.

    ``share`` above 1 is a hard failure: it would require the other four modes to
    contribute negative transportation margin. A share close to 1 is a softer
    warning that the remaining modes are left implausibly little.
    """
    return bound_check(pipeline_allocation(year, control_total, margins), margins)


def bound_check(
    allocation: pd.Series,
    margins: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    One mode's allocation against the published all-mode ceiling, per commodity.

    ``share`` above 1 is a hard failure: it would require the other four modes to
    contribute negative transportation margin. A share close to 1 is a softer
    warning that the remaining modes are left implausibly little.
    """
    name = str(allocation.name or 'mode')
    published = published_transport_by_commodity(margins).reindex(allocation.index)
    return pd.DataFrame(
        {
            name: allocation,
            'published_all_modes': published,
            'share': allocation / published,
            'headroom_other_modes': published - allocation,
        }
    ).sort_values(name, ascending=False)


# --- rail ------------------------------------------------------------------

#: The STCC5 -> BEA 2017 commodity map. Regenerated by
#: ``bedrock/utils/mapping/write_stcc5_bea_crosswalk.py``, which is where the
#: judgement lives; this file only consumes it.
RAIL_CROSSWALK_PATH = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'Crosswalk_STCC5_to_BEA_2017.csv'
)

#: BEA 2017 detail code for the rail commodity, whose own rows carry the margin
#: it gives up.
RAIL_COMMODITY = '482000'

#: Rows of the CRSR that are totals rather than commodities.
_CRSR_TOTAL_PATTERN = 'TOTAL|Percent'


def load_rail_crosswalk() -> pd.DataFrame:
    """The STCC5 -> BEA 2017 commodity map, excluded codes included.

    Rows with an empty ``bea_2017_commodity`` are the thirteen codes that name a
    service class or an empty move rather than a commodity. They are kept in the
    file, and dropped here, so the exclusion stays visible rather than looking
    like an oversight.
    """
    return pd.read_csv(RAIL_CROSSWALK_PATH, dtype=str).fillna('')


def load_rail_revenue_by_stcc(year: int) -> pd.Series:
    """Released rail revenue per STCC5 for *year*, USD, from ``STB_CRSR``."""
    fba = getFlowByActivity('STB_CRSR', year)
    items = fba[
        ~fba['ActivityProducedBy'].str.contains(
            _CRSR_TOTAL_PATTERN, case=False, na=False
        )
    ]
    return items.groupby('ActivityProducedBy')['FlowAmount'].sum()


def rail_revenue_by_commodity(
    year: int = 2017, margins: pd.DataFrame | None = None
) -> pd.Series:
    """
    Rail revenue per BEA 2017 commodity for *year*, USD.

    Revenue *is* the weight: all but one of the mapped STCC codes go to exactly
    one BEA commodity, so the within-set question pipeline needs a default for
    barely arises. The exception splits on published transport - see below.

    ⚠️ **Excluded codes are dropped and the rest carry the whole column.** The
    thirteen service-class and empty-move codes are 16.3% of released revenue,
    ``46111`` trailer-on-flatcar alone being 14.0%. Dropping them and letting the
    remainder renormalise is the treatment BEA described for the equivalent
    bucket on the *truck* side, so applying it to rail is an inference rather
    than something BEA told us - see the crosswalk writer's docstring.

    ⚠️ **Shares are computed on *released* revenue.** 4.7% of 2017 revenue is
    redacted to protect shippers, and the CRSR publishes the redacted mass only
    as an all-data total, never per commodity. Using released shares therefore
    assumes the redacted freight has the same commodity mix as the released
    freight. Only the shares matter - the level comes from the 2017 anchor - so
    this bites only to the extent redaction is concentrated in particular
    commodities, which by construction cannot be checked.
    """
    revenue = load_rail_revenue_by_stcc(year)
    crosswalk = load_rail_crosswalk()
    mapped = crosswalk[crosswalk['bea_2017_commodity'] != '']

    unknown = set(revenue.index) - set(crosswalk['stcc5'])
    if unknown:
        raise ValueError(
            f'STB_CRSR {year} carries {len(unknown)} STCC5 codes the crosswalk '
            f'does not list, e.g. {sorted(unknown)[:5]}. Every code must be '
            f'either mapped or explicitly excluded, so an unlisted one would be '
            f'silently dropped from the rail allocation. Re-run '
            f'bedrock/utils/mapping/write_stcc5_bea_crosswalk.py.'
        )

    joined = mapped.assign(revenue=mapped['stcc5'].map(revenue).astype(float)).dropna(
        subset=['revenue']
    )

    # Almost every STCC code maps to one BEA commodity, so revenue passes
    # straight through. A handful genuinely span two - 37112 is "motor trucks OR
    # truck tractors", which is BEA's light-truck and heavy-duty-truck
    # commodities at once - and those split on the published transport column,
    # the same default the pipeline sets use. Without this the shared revenue
    # would be counted once per target.
    published = published_transport_by_commodity(margins)
    weights = joined['bea_2017_commodity'].map(published).astype(float)
    denominator = weights.groupby(joined['stcc5'].to_numpy()).transform('sum')
    empty = pd.Series(denominator <= 0, index=joined.index)
    if empty.any():
        stranded = sorted(set(joined.loc[empty, 'stcc5'].astype(str)))
        raise ValueError(
            f'STCC codes {stranded} map only to commodities with no published '
            f'transportation margin, so their revenue has nowhere to go. Fix the '
            f'crosswalk rather than letting the revenue vanish.'
        )
    joined = joined.assign(
        revenue=joined['revenue'] * weights.values / denominator.values
    )

    return (
        joined.groupby('bea_2017_commodity')['revenue']
        .sum()
        .sort_values(ascending=False)
    )


def rail_margin_2017(margins: pd.DataFrame | None = None) -> float:
    """The margin ``482000`` gives up in 2017, USD. See :func:`pipeline_margin_2017`."""
    return _mode_give_up_2017(RAIL_COMMODITY, margins)


def rail_allocation(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
) -> pd.Series:
    """
    Rail margin per BEA 2017 commodity for *year*. USD, indexed by commodity.

    Each commodity takes its share of *control_total* in proportion to the rail
    revenue earned hauling it, which is BEA's stated basis: *"For rail, we
    purchase the Freight Commodity Statistics from the American Association of
    Railroads which gives us very detailed revenue by product shipped by rail."*

    *control_total* defaults to the 2017 published give-up, which makes 2017 an
    identity; a nowcast year passes its own.
    """
    if control_total is None:
        control_total = rail_margin_2017(margins)
    revenue = rail_revenue_by_commodity(year, margins)
    if revenue.sum() <= 0:
        raise ValueError(f'STB_CRSR {year} has no mapped rail revenue to allocate on.')
    return (revenue / revenue.sum() * control_total).rename('rail')


# --- truck -----------------------------------------------------------------

#: The SAS Table 8 group -> BEA 2017 commodity map. Regenerated by
#: ``bedrock/utils/mapping/write_sas_group_bea_crosswalk.py``.
TRUCK_CROSSWALK_PATH = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'Crosswalk_SAS_Group_to_BEA_2017.csv'
)

#: BEA 2017 detail code for the truck commodity.
TRUCK_COMMODITY = '484000'

#: SAS Table 8 prefixes the commodity rows; the suffix is the group name.
TRUCK_GROUP_PREFIX = 'Estimated Revenue by Commodities Handled: '

#: The row the eleven groups partition. Used to check that they still do.
TRUCK_TOTAL_ITEM = 'Total Motor Carrier Revenue'

#: The group BEA discards. See :func:`load_truck_group_revenue`.
TRUCK_OTHER_GOODS = 'Other goods'

#: NAICS 484 is the only industry in Table 8 with commodity rows.
TRUCK_NAICS = '484'


def load_truck_crosswalk() -> pd.DataFrame:
    """The SAS group -> BEA 2017 commodity map. One row per (group, commodity)."""
    return pd.read_csv(TRUCK_CROSSWALK_PATH, dtype=str).fillna('')


def load_truck_group_revenue(year: int) -> pd.Series:
    """
    Motor carrier revenue per SAS Table 8 commodity group for *year*, USD.

    ⚠️ **"Other goods" is dropped and the rest renormalised by the caller.** It
    is 32.4% of motor carrier revenue in 2017 and BEA does not use it: *"We do
    not use the 'other' commodity from SAS Table 8 since we have no information
    on what commodities it contains. Distributing it pro rata to the other 10
    would not change the result since we are creating weights with the data."*
    Only shares are spent, so dropping it and renormalising is arithmetically
    identical to spreading it pro rata - BEA's own statement, so this is the
    method rather than an approximation of it.

    ⚠️ **The hazardous-materials row is a cross-cut of the same revenue**, not an
    eleventh group, so it is excluded by the prefix filter rather than summed.

    Raises if the groups stop partitioning the published total, which is the
    check that Table 8's taxonomy still matches what BEA describes.
    """
    # AIES replaced the Service Annual Survey from data year 2023. Its parse
    # emits the eleven groups and the total under Table 8's own FlowName
    # strings, so everything below this dispatch is one implementation across
    # the seam - including the group names, which join the crosswalk unchanged.
    source = 'Census_AIES_MiscSector' if year >= FIRST_AIES_YEAR else 'Census_SAS'
    fba = getFlowByActivity(source, year)
    table8 = fba[
        fba['Description'].astype(str).str.startswith('Table 8')
        & (fba['ActivityProducedBy'].astype(str) == TRUCK_NAICS)
    ]
    if table8.empty:
        raise ValueError(
            f'{source} Table 8 has no NAICS {TRUCK_NAICS} rows for {year}. That '
            f'is the only industry in the sheet carrying commodity detail, so '
            f'without it there is no truck allocator.'
        )

    groups = table8[table8['FlowName'].str.startswith(TRUCK_GROUP_PREFIX)]
    revenue = (
        groups.assign(group=groups['FlowName'].str[len(TRUCK_GROUP_PREFIX) :])
        .groupby('group')['FlowAmount']
        .sum()
    )

    published_total = table8.loc[
        table8['FlowName'] == TRUCK_TOTAL_ITEM, 'FlowAmount'
    ].sum()

    # ⚠️ A suppressed group subtracts from a control total, which is not the
    # usual harmless case: the groups *are* the whole. 2022 suppresses
    # pharmaceutical and chemical products, and the shortfall against the
    # published total is exactly that cell - 18,004 $M - so it is recovered by
    # subtraction rather than left as a zero. Only a single suppressed group can
    # be recovered this way; two would be one equation in two unknowns.
    suppressed = groups.loc[groups['Suppressed'].notna(), 'FlowName'].str[
        len(TRUCK_GROUP_PREFIX) :
    ]
    shortfall = published_total - revenue.sum()
    if len(suppressed) == 1 and shortfall > 0:
        revenue.loc[suppressed.iloc[0]] = shortfall
    elif len(suppressed) > 1:
        raise ValueError(
            f'{source} Table 8 suppresses {len(suppressed)} commodity groups in '
            f'{year}: {sorted(suppressed)}. One can be recovered by subtraction '
            f'from the published total; several cannot, and treating them as zero '
            f'would understate every other group once the shares renormalise.'
        )

    if published_total and abs(revenue.sum() / published_total - 1) > 1e-6:
        raise ValueError(
            f'The Table 8 commodity groups sum to {revenue.sum():,.0f} against a '
            f'published {TRUCK_TOTAL_ITEM} of {published_total:,.0f} in {year}, from '
            f'{source}. '
            f'They are meant to partition it exactly - a gap means the hazardous '
            f'materials cross-cut has been swept in, or the taxonomy has changed.'
        )
    return revenue


def truck_margin_2017(margins: pd.DataFrame | None = None) -> float:
    """The margin ``484000`` gives up in 2017, USD. See :func:`pipeline_margin_2017`."""
    return _mode_give_up_2017(TRUCK_COMMODITY, margins)


def truck_allocation(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
    within_group_weight: pd.Series | None = None,
) -> pd.Series:
    """
    Truck margin per BEA 2017 commodity for *year*. USD, indexed by commodity.

    Each of the ten identified groups takes its share of *control_total*, and
    within a group the split falls to *within_group_weight*, defaulting to the
    published transport column.

    ⚠️ **The within-group weight does more work here than anywhere else.** Ten
    groups span 258 commodities, against rail's near one-to-one mapping, so most
    of truck's commodity detail comes from the weight rather than from Table 8.
    That is a property of the source - BEA calls it *"a very aggregated level"* -
    not of this implementation, and BEA faces it too.
    """
    if control_total is None:
        control_total = truck_margin_2017(margins)

    revenue = load_truck_group_revenue(year).drop(index=TRUCK_OTHER_GOODS)
    shares = revenue / revenue.sum()
    basis = (
        published_transport_by_commodity(margins)
        if within_group_weight is None
        else within_group_weight
    )
    crosswalk = load_truck_crosswalk()

    unknown = set(shares.index) - set(crosswalk['sas_group'])
    if unknown:
        raise ValueError(
            f'SAS Table 8 publishes groups the crosswalk does not map: '
            f'{sorted(unknown)}. Each identified group carries part of the truck '
            f'margin, so an unmapped one would silently shrink the column.'
        )

    allocation: dict[str, float] = {}
    for group, share in shares.items():
        commodities = list(
            crosswalk.loc[crosswalk['sas_group'] == group, 'bea_2017_commodity']
        )
        weights = basis.reindex(commodities).fillna(0.0)
        if weights.sum() <= 0:
            raise ValueError(
                f'SAS group {group!r} maps to {len(commodities)} commodities, none '
                f'of which receives transportation margin in the published 2017 '
                f'table, so there is no basis on which to split its margin.'
            )

        group_margin = float(share) * control_total
        for commodity, weight in (weights / weights.sum()).items():
            allocation[commodity] = (
                allocation.get(commodity, 0.0) + group_margin * weight
            )

    return pd.Series(allocation, name='truck').sort_values(ascending=False)


# --- water and air ---------------------------------------------------------

#: SCTG -> BEA 2017 commodity, from the ported FAF crosswalk. The BEA code sits
#: in the ``Note`` column: the sector-mapping machinery joins on ``Sector``,
#: which is NAICS, so the BEA codes are read directly here instead (#546).
FAF_SCTG_CROSSWALK_PATH = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'activitytosectormapping'
    / 'Sector_Crosswalk_BTS_FAF_Mode_and_SCTG.csv'
)

#: The reconstructed 1/2/3 difficulty weights, per SCTG and mode.
FAF_MULTIPLIER_PATH = (
    Path(__file__).resolve().parents[2]
    / 'utils'
    / 'mapping'
    / 'Crosswalk_FAF_SCTG_Difficulty_Multiplier.csv'
)

#: FAF's mode label and BEA commodity, for the two volume-allocated modes.
VOLUME_MODES = {
    'water': ('Water', '483000', 'water_multiplier'),
    'air': ('Air (include truck-air)', '481000', 'air_multiplier'),
}

TON_MILES_UNIT = 'ton-miles'


def load_faf_sctg_crosswalk() -> pd.DataFrame:
    """SCTG -> BEA 2017 commodity pairs, deduplicated."""
    crosswalk = pd.read_csv(FAF_SCTG_CROSSWALK_PATH, dtype=str).fillna('')
    sctg = crosswalk[crosswalk['ActivitySourceName'] == 'FAF_SCTG']
    return (
        sctg[['Activity', 'Note']]
        .rename(columns={'Activity': 'sctg', 'Note': 'bea_2017_commodity'})
        .query("bea_2017_commodity != ''")
        .drop_duplicates()
        .reset_index(drop=True)
    )


def load_difficulty_multipliers() -> pd.DataFrame:
    """The reconstructed 1/2/3 weights per SCTG, one column per mode."""
    return pd.read_csv(FAF_MULTIPLIER_PATH)


def load_faf_ton_miles(mode: str, year: int) -> pd.Series:
    """FAF ton-miles for one mode and year, per SCTG."""
    fba = getFlowByActivity('BTS_FAF', year)
    rows = fba[
        (fba['ActivityProducedBy'].astype(str) == mode)
        & (fba['Unit'] == TON_MILES_UNIT)
    ]
    if rows.empty:
        raise ValueError(
            f'BTS_FAF {year} has no {TON_MILES_UNIT} for mode {mode!r}. Water and '
            f'air are the only modes BEA allocates on volume, so without this '
            f'there is no allocator for them.'
        )
    return rows.groupby('FlowName')['FlowAmount'].sum()


def volume_mode_margin_2017(
    mode_name: str, margins: pd.DataFrame | None = None
) -> float:
    """The margin the water or air commodity gives up in 2017, USD."""
    _, commodity, _ = VOLUME_MODES[mode_name]
    return _mode_give_up_2017(commodity, margins)


def volume_mode_allocation(
    mode_name: str,
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
    within_sctg_weight: pd.Series | None = None,
) -> pd.Series:
    """
    Water or air margin per BEA 2017 commodity for *year*. USD.

    The allocator is BEA's weighted ton-mile share - ``m_c * tonmiles_c`` over
    its sum, with ``m`` the 1/2/3 difficulty multiplier - computed per SCTG, then
    spread across each SCTG's BEA commodities.

    ⚠️ **Two weights stack here, and only the first is BEA's.** The difficulty
    multiplier is BEA's own construction, reconstructed from the rule they gave.
    The split *within* an SCTG is ours: FAF publishes 42 groups against 258
    receiving commodities, so the same aggregation problem truck has appears
    again, and it falls to the published transport column by default.

    Water and air are 2.3% and 1.5% of ``TRANS``, so this compounding is bounded
    - which is why the unpublished multiplier table was never the blocker it
    looked like.
    """
    faf_mode, _, multiplier_column = VOLUME_MODES[mode_name]
    if control_total is None:
        control_total = volume_mode_margin_2017(mode_name, margins)

    ton_miles = load_faf_ton_miles(faf_mode, year)
    multipliers = load_difficulty_multipliers().set_index('sctg')[multiplier_column]

    missing = set(ton_miles.index) - set(multipliers.index)
    if missing:
        raise ValueError(
            f'FAF publishes SCTG groups with no difficulty multiplier: '
            f'{sorted(missing)}. Defaulting them would silently weight a commodity '
            f'BEA may treat as hard to handle the same as bulk grain.'
        )

    weighted = ton_miles * multipliers.reindex(ton_miles.index)
    shares = weighted / weighted.sum()

    basis = (
        published_transport_by_commodity(margins)
        if within_sctg_weight is None
        else within_sctg_weight
    )
    crosswalk = load_faf_sctg_crosswalk()

    allocation: dict[str, float] = {}
    for sctg, share in shares.items():
        commodities = list(
            crosswalk.loc[crosswalk['sctg'] == sctg, 'bea_2017_commodity']
        )
        weights = basis.reindex(commodities).fillna(0.0)
        if weights.sum() <= 0:
            # an SCTG whose commodities bear no transport margin cannot place its
            # share; that share is dropped and the rest renormalise below
            continue
        sctg_margin = float(share) * control_total
        for commodity, weight in (weights / weights.sum()).items():
            allocation[commodity] = (
                allocation.get(commodity, 0.0) + sctg_margin * weight
            )

    series = pd.Series(allocation, name=mode_name)
    if series.sum() <= 0:
        raise ValueError(f'No {mode_name} margin could be placed for {year}.')
    return (series / series.sum() * control_total).sort_values(ascending=False)


def water_allocation(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
) -> pd.Series:
    """Water margin per BEA 2017 commodity. See :func:`volume_mode_allocation`."""
    return volume_mode_allocation('water', year, control_total, margins)


def air_allocation(
    year: int = 2017,
    control_total: float | None = None,
    margins: pd.DataFrame | None = None,
) -> pd.Series:
    """Air margin per BEA 2017 commodity. See :func:`volume_mode_allocation`."""
    return volume_mode_allocation('air', year, control_total, margins)


# --- annual control totals -------------------------------------------------

#: The margins anchor year, where every mode's give-up is published.
ANCHOR_YEAR = 2017

#: The five transport commodities, by mode name.
MODE_COMMODITIES = {
    'truck': TRUCK_COMMODITY,
    'rail': RAIL_COMMODITY,
    'pipeline': PIPELINE_COMMODITY,
    'water': VOLUME_MODES['water'][1],
    'air': VOLUME_MODES['air'][1],
}

#: Air and water freight NAICS in SAS Table 2. Their *industry* output is mostly
#: passengers - the 2017 margin is 2.6% of air's output and 17.3% of water's -
#: so the freight lines have to be taken on their own rather than the parent.
#:
#: ``481219`` other nonscheduled air is left out: it is air taxi and sightseeing
#: as much as freight, and at 1,936 $M in 2017 it is not worth the ambiguity.
AIR_FREIGHT_NAICS = ('481112', '481212')
WATER_FREIGHT_NAICS = ('483111', '483113', '483211')

#: Every mode whose annual freight revenue is observed.
FREIGHT_REVENUE_MODES = ('truck', 'rail', 'pipeline', 'water', 'air')

#: The freight NAICS per mode, for the two that need selecting out of a parent.
_SAS_FREIGHT_NAICS = {'air': AIR_FREIGHT_NAICS, 'water': WATER_FREIGHT_NAICS}

#: The row carrying rail revenue inclusive of redacted cells.
_CRSR_ALL_DATA = 'TOTALS (All Data)'


def mode_freight_revenue(mode_name: str, year: int) -> float:
    """
    Observed freight revenue for *mode_name* in *year*, USD.

    This is the annual quantity that moves the control total. It comes from the
    same source that does the mode's commodity allocation, so the level and the
    shape cannot drift apart.
    """
    if mode_name == 'truck':
        return float(load_truck_group_revenue(year).sum())
    if mode_name == 'pipeline':
        return float(load_pipeline_item_revenue(year).sum())
    if mode_name == 'rail':
        fba = getFlowByActivity('STB_CRSR', year)
        rows = fba[fba['ActivityProducedBy'].astype(str).str.strip() == _CRSR_ALL_DATA]
        if rows.empty:
            raise ValueError(
                f'STB_CRSR {year} has no {_CRSR_ALL_DATA!r} row. That is the only '
                f'figure inclusive of the redacted cells, which are 4.7% of rail '
                f'revenue, so the released total would understate the control.'
            )
        return float(rows['FlowAmount'].sum())
    if mode_name == 'air' and int(year) > _AIR_LAST_OBSERVED_REVENUE_YEAR:
        return air_revenue_from_volume(year)
    if mode_name in _SAS_FREIGHT_NAICS:
        codes = _SAS_FREIGHT_NAICS[mode_name]
        # Same seam as pipeline: SAS Table 2's detailed-NAICS revenue continues
        # on timeseries/aies/basic from 2023. All five freight NAICS are
        # published there, so neither mode falls back to a parent industry.
        if year >= FIRST_AIES_YEAR:
            source = 'Census_AIES'
            fba = getFlowByActivity(source, year)
            table2 = fba[fba['FlowName'].astype(str).str.strip() == 'Sales']
        else:
            source = 'Census_SAS'
            fba = getFlowByActivity(source, year)
            table2 = fba[fba['Description'].astype(str).str.startswith('Table 2')]
        revenue = table2[table2['ActivityProducedBy'].astype(str).isin(codes)]
        found = set(revenue['ActivityProducedBy'].astype(str))
        if found != set(codes):
            raise ValueError(
                f'{source} is missing freight NAICS {sorted(set(codes) - found)} '
                f'for {mode_name} in {year}. Falling back to the parent industry '
                f'would put passenger revenue into a freight control - air is 2.6% '
                f'margin on its output, so that error would be an order of magnitude.'
            )
        return float(revenue['FlowAmount'].sum())

    raise NotImplementedError(f'{mode_name} has no observed annual freight revenue.')


def _published_air_revenue(year: int) -> float:
    """Air freight revenue as published, USD. The 2022 anchor for the volume index."""
    codes = _SAS_FREIGHT_NAICS['air']
    fba = getFlowByActivity('Census_SAS', int(year))
    table2 = fba[fba['Description'].astype(str).str.startswith('Table 2')]
    revenue = table2[table2['ActivityProducedBy'].astype(str).isin(codes)]
    found = set(revenue['ActivityProducedBy'].astype(str))
    if found != set(codes):
        raise ValueError(
            f'Census_SAS Table 2 is missing air freight NAICS '
            f'{sorted(set(codes) - found)} for {year}, which anchors the volume '
            f'index.'
        )
    return float(revenue['FlowAmount'].sum())


#: Last year air's freight revenue is taken as published. AIES 2023 is
#: contradicted by air's own volume series - see :func:`air_revenue_from_volume`.
_AIR_LAST_OBSERVED_REVENUE_YEAR = 2022


def air_revenue_from_volume(year: int) -> float:
    """Air freight revenue for *year*, USD, moved on FAF ton-miles.

    ⚠️ **Air is the one mode whose published revenue is not used after 2022**, and
    the reason is that its own allocation basis contradicts it.

    ``481212`` nonscheduled chartered freight air runs 4,846 / 4,857 / 4,987 /
    6,045 $M across 2019-2022 in SAS Table 2 - unsuppressed - and AIES 2023
    publishes 13,271 $M. Taken with ``481112`` that puts air freight revenue at
    **2.32x** its 2017 level. FAF ton-miles for the same mode put volume at
    **1.06x**, having fallen back from 1.32x in 2022, so the published revenue
    implies unit revenue **doubling in a single year** - in the year air cargo
    rates collapsed from their pandemic peak. The move is the wrong size and the
    wrong sign.

    So from 2023 air's control moves on volume instead, holding unit revenue at
    its last observed value::

        revenue(year) = revenue(2022) x ton_miles(year) / ton_miles(2022)

    **Volume is not a new source here** - FAF ton-miles is already air's
    commodity allocator, so this uses what the mode is built on rather than
    importing a fourth series to arbitrate.

    ⚠️ **Holding unit revenue flat is deliberately conservative.** Air cargo
    rates *fell* in 2023, so this still overstates the level somewhat; correcting
    for the rate move would need a rate series the build does not carry, and
    inventing one would be a worse error than the one being fixed.

    ⚠️ **Water is not treated this way and must not be.** Its unit revenue moves
    -4% across the same seam (1.65 -> 1.58), which is continuous. Only air breaks.

    ⚠️ **Revisit when AIES publishes 2024.** If ``481212`` stays at the new level
    it is a re-based series rather than a bad year, and the right fix becomes
    re-anchoring rather than indexing off 2022.
    """
    anchor = _AIR_LAST_OBSERVED_REVENUE_YEAR
    if int(year) <= anchor:
        raise ValueError(
            f'air_revenue_from_volume is for years after {anchor}; {year} has a '
            f'published SAS revenue and should use it.'
        )
    label = VOLUME_MODES['air'][0]
    volume_now = float(load_faf_ton_miles(label, int(year)).sum())
    volume_anchor = float(load_faf_ton_miles(label, anchor).sum())
    if volume_anchor <= 0:
        raise ValueError(
            f'BTS_FAF has no {anchor} air ton-miles, so air revenue cannot be '
            f'indexed off it.'
        )
    return _published_air_revenue(anchor) * volume_now / volume_anchor


def mode_coverage_ratio(mode_name: str, margins: pd.DataFrame | None = None) -> float:
    """
        The mode's 2017 margin divided by its 2017 freight revenue.

        Near 1 by construction: for a freight mode essentially all revenue is margin,
        because the transport cost of moving a good to its buyer is unbundled and
        shifted forward onto that good - *"the treatment of trade margins parallels
        the treatments of transportation costs... which are also unbundled and
        shifted forward regardless of who actually pays the costs"* (BEA IO manual
        2009, ch. 2).

    ⚠️ **The ratio means two different things across the five modes**, and only the
        first is a small correction.

        For the three land modes it is near unity and is a *coverage* adjustment:
        1.042 truck, 0.995 rail, 1.052 pipeline. Rail is nearest because the STB
        waybill sample covers essentially all Class I traffic; truck and pipeline run
        above it in the same direction because SAS covers **employer firms**, leaving
        owner-operators and private carriage outside a margin that includes them.

        For water and air it is roughly half - 0.478 and 0.584 - and is doing a much
        bigger job: **their freight revenue includes international legs, while the
        margin is the domestic leg only**, the foreign leg sitting in ``MCIF``. That
        is a larger thing to freeze, and it would move if the domestic/international
        mix shifted. Two things bound the risk: the modes are 3.8% of ``TRANS``
        between them, and the split does not appear to be a distinct source of
        movement - deep sea, domestic and all-freight water revenue have almost
        identical year-on-year volatility (0.146, 0.147, 0.141), so dropping deep sea
        changes the level of the ratio but barely the shape of the series.

        ⚠️ **Freezing this ratio at 2017 is the whole modelling content of the annual
        control.** It assumes source coverage is stable over time, which is a much
        smaller claim than choosing among constructions that disagreed by 11.8%, but
        it is still an assumption and nothing here tests it.

        ⚠️ **It is a weak choice and is kept only for now.** For water and air the
        ratio is not a coverage correction at all - it is standing in for a
        domestic/international split that genuinely moves, and freezing a thing
        that moves is the weakest link in the annual control. It survives on
        bounded blast radius (the two modes are 3.8% of ``TRANS``) rather than on
        evidence that it holds. Replacing it needs an observed domestic leg, not
        a different frozen number; until then this is a known soft spot rather
        than a settled method. The air correction above is a symptom of the same
        gap: a frozen ratio passes any error in the revenue series straight
        through to the margin.
    """
    give_up = _mode_give_up_2017(MODE_COMMODITIES[mode_name], margins)
    revenue = mode_freight_revenue(mode_name, ANCHOR_YEAR)
    return give_up / revenue


def mode_control_total(
    mode_name: str, year: int, margins: pd.DataFrame | None = None
) -> float:
    """
    The margin *mode_name* gives up in *year*, USD - the level to allocate.

    ``coverage ratio (frozen at 2017) x observed freight revenue in year``, so
    *year* = 2017 reproduces the published give-up exactly.

    This replaces the three constructions the retired ton-mile chain used -
    ``residual``, ``output_ratio`` and ``freight_volume`` - which agreed in 2017
    and spread 11.8% by 2022. It avoids all three of their problems: no Use
    matrix, so no circularity with Step 6b; no industry-versus-commodity output
    gap, since revenue is neither; and no passenger contamination.
    """
    return mode_coverage_ratio(mode_name, margins) * mode_freight_revenue(
        mode_name, year
    )


def control_total_table(
    years: Iterable[int], margins: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Annual control totals per mode, USD, for the modes that have a source."""
    return pd.DataFrame(
        {
            mode: {year: mode_control_total(mode, year, margins) for year in years}
            for mode in FREIGHT_REVENUE_MODES
        }
    ).rename_axis('year')


# --- the Supply table's TRANS column ---------------------------------------


def mode_allocations(
    year: int = ANCHOR_YEAR, margins: pd.DataFrame | None = None
) -> dict[str, pd.Series]:
    """
    Every mode's commodity allocation for *year*, USD, on its own basis.

    Each is controlled to that mode's own annual total, so the five together
    carry the whole column.
    """
    controls = {
        mode: mode_control_total(mode, year, margins) for mode in FREIGHT_REVENUE_MODES
    }
    return {
        'truck': truck_allocation(year, controls['truck'], margins),
        'rail': rail_allocation(year, controls['rail'], margins),
        'pipeline': pipeline_allocation(year, controls['pipeline'], margins),
        'water': volume_mode_allocation('water', year, controls['water'], margins),
        'air': volume_mode_allocation('air', year, controls['air'], margins),
    }


def transport_margin_column(
    year: int = ANCHOR_YEAR, margins: pd.DataFrame | None = None
) -> pd.Series:
    """
    The Supply table's ``TRANS`` column for *year*. USD, by BEA 2017 commodity.

    Positive on the commodities that *receive* transport margin, negative on the
    five transport commodities that give it up, and **summing to zero** - margin
    is a redistribution, not value created, which is target T16's identity and
    the only constraint the balance places on Step 4c's own output.

    ⚠️ **In a nowcast year there is no published column to violate.** The
    per-commodity overshoot measured against 2017 is a statement about how
    faithfully we reproduce BEA's benchmark, not a defect in this column: the
    five mode totals are each right, the signs are right, and the identity holds
    for any year. What the overshoot says is that our *distribution* differs from
    BEA's by about 11% of the column in 2017, concentrated in commodities where
    one mode's observed detail collides with another's inferred detail.

    So this is usable now and improvable later: whatever resolves the collision -
    BEA's within-group rule, an exclusion list, or a joint solve - changes how
    the positive side is distributed without changing its total, its sign
    pattern, or the identity.
    """
    allocations = mode_allocations(year, margins)
    receiving = pd.DataFrame(allocations).fillna(0.0).sum(axis=1).rename('TRANS')

    given_up = pd.Series(
        {
            MODE_COMMODITIES[mode]: -allocation.sum()
            for mode, allocation in allocations.items()
        }
    )
    overlap = set(receiving.index) & set(given_up.index)
    if overlap:
        raise ValueError(
            f'Transport commodities {sorted(overlap)} appear on both sides of the '
            f'column. A mode may not deliver margin to a transport commodity - BEA '
            f'publishes zero transport margin received by all five in 2017 - so '
            f'this is a crosswalk error, not a rounding one.'
        )

    column = pd.concat([receiving, given_up]).rename('TRANS').sort_index()
    residual = column.sum()
    if abs(residual) > 1.0:
        raise ValueError(
            f'The TRANS column for {year} sums to {residual:,.2f} rather than zero. '
            f'Margin is a redistribution, so target T16 requires the column to net '
            f'out; a non-zero sum means a mode total and its allocation disagree.'
        )
    return column


def mode_residual(
    allocations: dict[str, pd.Series],
    margins: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    What the unbuilt modes must still supply, per commodity. USD.

    The five modes are a decomposition of one published column, so they are not
    independent: for every commodity

    .. code-block::

        sum over modes of allocation[mode, commodity] = TRANS[commodity]

    A mode built on its own - pipeline today - is therefore **provisional**. It
    is only finally right when the remaining four are built and the five are
    reconciled against the published column, and any mode that overshoots forces
    a negative onto the others.

    Pass whatever modes exist as ``{mode_name: allocation}``. ``residual`` is
    what is left for the rest; ``over_allocated`` marks commodities where the
    built modes already exceed the published column, which is a hard error
    rather than a tolerance - transport margin is never negative outside the
    ``F03000`` inventory rows, and those are buyer-level, not commodity-level.
    """
    published = published_transport_by_commodity(margins)
    built = pd.DataFrame(allocations).reindex(published.index).fillna(0.0)
    residual = published - built.sum(axis=1)
    frame = built.assign(
        published_all_modes=published,
        residual=residual,
        over_allocated=residual < 0,
    )
    return frame.loc[built.sum(axis=1) > 0].sort_values('residual')
