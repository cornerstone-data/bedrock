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
truck        67.8%   revenue by commodity group, SAS Table 8    not built
rail         16.5%   revenue by product, AAR / STB              not built
**pipeline** 11.9%   four Census margin items -> commodity sets **built**
water         2.3%   ton-miles x difficulty multiplier          not built
air           1.5%   ton-miles x difficulty multiplier          not built
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
annually in **Service Annual Survey Table 2**, 2013-2022. Each maps to a named
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
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.transform.iot.nowcast_margins import (
    COMMODITY_LEVEL,
    load_margins_transactions_2017,
)

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
    fba = getFlowByActivity('Census_SAS', year)
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
            f'Census_SAS Table 2 has no pipeline revenue for {missing} in {year}. '
            f'Those are BEA pipeline margin items; a missing one would silently '
            f'drop its commodity set from the allocation.'
        )

    published_total = table2[table2['ActivityProducedBy'] == PIPELINE_TOTAL_CODE][
        'FlowAmount'
    ].sum()
    if published_total and abs(revenue.sum() / published_total - 1) > 1e-6:
        raise ValueError(
            f'The four pipeline margin items sum to {revenue.sum():,.0f} against a '
            f'published NAICS 486 total of {published_total:,.0f} in {year}. They '
            f'are meant to partition it exactly - a gap means the detailed NAICS '
            f'no longer match the four items BEA named.'
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
    df = load_margins_transactions_2017() if margins is None else margins
    rows = df.loc[df.index.get_level_values(COMMODITY_LEVEL) == PIPELINE_COMMODITY]
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
    allocation = pipeline_allocation(year, control_total, margins)
    published = published_transport_by_commodity(margins).reindex(allocation.index)
    return pd.DataFrame(
        {
            'pipeline': allocation,
            'published_all_modes': published,
            'share': allocation / published,
            'headroom_other_modes': published - allocation,
        }
    ).sort_values('pipeline', ascending=False)


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
