"""
Clean functions for the ``Margins_Transport_<year>`` FBS (#611, step 4c).

The method splits the Supply table's ``TRANS`` column by the transport
commodity that gives the margin up, so the nowcast can write an expanded
margins file with one column per mode rather than a single transport total.

**The division of labour with**
:mod:`bedrock.transform.iot.nowcast_transport_margins` **is deliberate.**

- The *module* owns each mode's **allocator series** and its **control total**.
  Those are where the source-reading judgement lives: SAS's suppressed truck
  group recovered by subtraction, the "other goods" group BEA discards, the
  STCC codes that name a service class rather than a commodity, the 1/2/3
  difficulty multiplier, and the 2017-anchored coverage ratio that turns
  observed freight revenue into a margin level. None of that is expressible in
  a method yaml, and duplicating it here would let the two drift.
- The *FBS* owns the **split from the source's own key to BEA commodities** -
  ten SAS groups, 498 STCC codes, four pipeline items and 42 SCTG groups onto
  258 receiving commodities - via the crosswalks and proportional attribution
  against the published 2017 ``TRANS`` column. That is the part the attribution
  machinery does better than a loop, and it is what makes the result a
  versioned artefact with data-quality columns rather than a function return.

``test_margins_transport.py`` asserts the two paths agree per commodity, which
is what keeps the division honest.

⚠️ **Every activity set is keyed by one ``clean_parameter``: the mode name**
(``truck``, ``rail``, ``pipeline``, ``water``, ``air``). It selects the
allocator before mapping and the control total after aggregation, so a set
cannot be half-configured for one mode and half for another.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.transform.iot import nowcast_transport_margins as tm
from bedrock.utils.logging.flowsa_log import log

#: The unit every allocator is expressed in once it reaches the FBS. Water and
#: air arrive as weighted ton-miles and only become dollars at the control-total
#: step, which is exactly where the unit changes.
USD = 'USD'

#: What flows, per mode: the transport commodity whose margin it is. The
#: allocator key - an STCC code, a SAS group - identifies the *source row*, not
#: the flow, so it stays in ``ActivityProducedBy`` where the crosswalk reads it
#: and out of ``FlowName``. Holding it constant within a mode is also what lets
#: the final aggregation collapse 498 STCC codes onto the commodities they
#: serve, which is the one row per (commodity, mode) the expanded margins file
#: wants; naming it for the mode is what makes that row readable without
#: looking ``SectorConsumedBy`` up in the BEA code list.
FLOWABLE = {
    'truck': 'Truck transportation',
    'rail': 'Rail transportation',
    'pipeline': 'Pipeline transportation',
    'water': 'Water transportation',
    'air': 'Air transportation',
}


def _mode(fb: Any) -> str:
    """The mode this activity set is for, from its ``clean_parameter``."""
    mode = fb.config.get('clean_parameter')
    if mode not in tm.MODE_COMMODITIES:
        raise ValueError(
            f'clean_parameter must name one of the five transport modes '
            f'{sorted(tm.MODE_COMMODITIES)}, not {mode!r}. It is what selects both '
            f'the allocator and the control total, so an activity set without it '
            f'would silently allocate one mode on another mode\'s basis.'
        )
    return str(mode)


def _year(fba: pd.DataFrame) -> int:
    years = set(fba['Year'].astype(int))
    if len(years) != 1:
        raise ValueError(
            f'Expected a single year in the activity set, found {sorted(years)}. '
            f'The control total is annual, so mixing years would apply one year\'s '
            f'level to another year\'s shape.'
        )
    return years.pop()


def _rebuild(
    fba: pd.DataFrame, mode: str, allocator: pd.Series, unit: str
) -> pd.DataFrame:
    """
    Replace the activity set's rows with one row per allocator key.

    The first selected row is the template, so ``Location``, ``Year``, ``Class``
    and the rest of the FBA schema carry through unchanged; only the activity,
    the amount and the unit are rewritten.
    """
    if fba.empty:
        raise ValueError(
            'The activity set selected no rows, so there is nothing to attach the '
            'allocator to. Check selection_fields against the FBA.'
        )
    template = fba.iloc[[0]].copy()
    rows = pd.concat([template] * len(allocator), ignore_index=True).assign(
        ActivityProducedBy=list(allocator.index.astype(str)),
        ActivityConsumedBy=None,
        FlowName=FLOWABLE[mode],
        FlowAmount=list(allocator.to_numpy(dtype=float)),
        Unit=unit,
        # margin dollars are a technosphere flow whichever source they came
        # from; Census_SAS labels its revenue ELEMENTARY_FLOW, which would
        # otherwise split the output into two flow types for no reason
        FlowType='TECHNOSPHERE_FLOW',
    )
    if 'Suppressed' in rows.columns:
        # suppression is resolved inside the module's loader - a value that got
        # here has already been recovered or the loader raised
        rows['Suppressed'] = None
    return rows


def mode_allocator(fba: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """
    ``clean_fba_before_mapping``: the mode's allocator, keyed by its own code.

    One row per SAS commodity group (truck), STCC5 code (rail), pipeline margin
    item, or SCTG group (water and air). The crosswalk and proportional
    attribution then carry each key onto the BEA commodities it serves.
    """
    mode = _mode(fba)
    year = _year(fba)

    if mode == 'truck':
        allocator = tm.load_truck_group_revenue(year).drop(
            index=tm.TRUCK_OTHER_GOODS, errors='ignore'
        )
        unit = USD
    elif mode == 'rail':
        allocator = _rail_allocator(year)
        unit = USD
    elif mode == 'pipeline':
        allocator = tm.load_pipeline_item_revenue(year)
        unit = USD
    else:
        allocator = _volume_allocator(mode, year)
        unit = tm.TON_MILES_UNIT

    log.info(
        f'Margins_Transport {mode} {year}: {len(allocator)} allocator keys, '
        f'{allocator.sum():,.0f} {unit}'
    )
    return _rebuild(fba, mode, allocator, unit)


def _rail_allocator(year: int) -> pd.Series:
    """
    Released rail revenue per STCC5, excluded codes dropped.

    ⚠️ The unknown-code guard is repeated here rather than inherited: the
    crosswalk the FBS reads is a *re-emission* of the module's, so a code added
    to the source workbook but not to either file would otherwise be dropped by
    the mapping without a word.
    """
    revenue = tm.load_rail_revenue_by_stcc(year)
    crosswalk = tm.load_rail_crosswalk()
    unknown = set(revenue.index) - set(crosswalk['stcc5'])
    if unknown:
        raise ValueError(
            f'STB_CRSR {year} carries {len(unknown)} STCC5 codes neither mapped nor '
            f'excluded, e.g. {sorted(unknown)[:5]}. Re-run '
            f'bedrock/utils/mapping/write_stcc5_bea_crosswalk.py, then '
            f'write_margins_sector_crosswalks.py.'
        )
    mapped = set(crosswalk.loc[crosswalk['bea_2017_commodity'] != '', 'stcc5'])
    return revenue[revenue.index.isin(mapped)]


def _volume_allocator(mode: str, year: int) -> pd.Series:
    """
    Difficulty-weighted ton-miles per SCTG for water or air.

    ``m_c * tonmiles_c`` with ``m`` the 1/2/3 multiplier BEA described. The
    multiplier is a per-SCTG weight applied *before* attribution, which the
    attribution model has no slot for - so it is applied here, on the source
    side, rather than dressed up as an attribution source.
    """
    faf_mode, _, multiplier_column = tm.VOLUME_MODES[mode]
    ton_miles = tm.load_faf_ton_miles(faf_mode, year)
    multipliers = tm.load_difficulty_multipliers().set_index('sctg')[multiplier_column]

    missing = set(ton_miles.index) - set(multipliers.index)
    if missing:
        raise ValueError(
            f'FAF publishes SCTG groups with no difficulty multiplier: '
            f'{sorted(missing)}. Defaulting them would silently weight a commodity '
            f'BEA may treat as hard to handle the same as bulk grain.'
        )
    return ton_miles * multipliers.reindex(ton_miles.index)


def scale_to_control_total(fbs: pd.DataFrame, **_: Any) -> pd.DataFrame:
    """
    ``clean_fbs_after_aggregation``: put the mode's level and its own code on.

    Two things happen here, and both have to happen after attribution:

    - **The level.** Attribution preserves the allocator's total - revenue, or
      weighted ton-miles - not the margin. ``mode_control_total`` is the frozen
      2017 coverage ratio times observed freight revenue, so rescaling to it
      makes 2017 reproduce the published give-up exactly and a nowcast year move
      with the source. For water and air this is also where weighted ton-miles
      become dollars, which is why ``Unit`` is set rather than assumed.
    - **The mode.** ``SectorConsumedBy`` takes the transport commodity that
      gives the margin up. It cannot be set earlier: these are
      ``TECHNOSPHERE_FLOW`` rows, and ``add_primary_secondary_columns``
      prioritises ConsumedBy for that flow type, so a populated ConsumedBy
      sector would capture ``PrimarySector`` and the proportional split would
      run against the mode instead of the commodity. NIPA_final_dom_uses
      documents the same trap at issue #539.
    """
    mode = _mode(fbs)
    commodity = tm.MODE_COMMODITIES[mode]
    year = _year(fbs)

    total = float(fbs['FlowAmount'].sum())
    if total <= 0:
        raise ValueError(
            f'The {mode} activity set attributed {total:,.0f} in {year}, so there '
            f'is no shape to scale. A mode with no allocator reaching a commodity '
            f'would put its whole control total nowhere.'
        )

    control = tm.mode_control_total(mode, year)
    log.info(
        f'Margins_Transport {mode} {year}: scaling {total:,.0f} to a control total '
        f'of {control:,.0f} USD'
    )
    return fbs.assign(
        FlowAmount=fbs['FlowAmount'] * control / total,
        SectorConsumedBy=commodity,
        Unit=USD,
        # BTS_FAF is classed Other because it publishes ton-miles; once the
        # control total has turned those into dollars the whole method is Money
        Class='Money',
    )
