"""Nowcasted US national Make/Use/Import tables.

Goal: build a full national Make, Use, and Import table for a series of years,
assembled from independently-sourced sections, ultimately converted to the
Cornerstone schema (after redefinitions) and RAS-rebalanced against known
controls. See https://github.com/orgs/cornerstone-data/projects/26 for the tracking board.

This module implements the final-demand section of the Use table, in
purchaser (PUR) price, commodity x SUT final-demand codes (MUT list minus
``F05000``), the Supply bridge block (commodity x the 12 basic-to-
purchaser codes), and the Use table's 402 x 402 intermediate interior
(``derive_initial_U_intermediate``, Step 3 / #497 - the sourcing lives in
``bedrock.transform.iot.nowcast_intermediate``). Y source: the ``NIPA_final_dom_uses_<year>`` FBS methods
(``bedrock/transform/nipa/NIPA_final_dom_uses_<year>.yaml``) plus, for 2017,
``Trade_Exports_<year>`` on ``F04000`` via mapped Detail mass in
``_trade_fbs_commodity_vector``. ``F03000`` (change in private
inventories, #529) is present but all-zero. ``F05000`` is MUT-only and
is not a Y column. The Supply bridge fills ``MCIF`` from
``Trade_Imports_<year>`` (same mapped Detail mass), ``MDTY`` from Census
duty rates leveled to NIPA ``B235RC``, and ``MADJ`` from Census import
charges (``GEN_CHA_YR``) reassigned onto 2017 Supply ``MADJ``
destination codes and leveled to published Supply ``MADJ``, for 2017.
``TRADE``/``TRANS`` are step 4c's margin columns and ``TOP``/``SUB`` are
step 4d's tax and subsidy columns, all sourced for a run of years. ⚠️
``SUB`` is stored negative, as BEA publishes it in the Supply table. ITA G+S scale lives in
``bedrock.transform.trade.scale`` and is not applied here (#647).

Each ``NIPA_final_dom_uses_<year>.yaml`` activity_set assigns its official BEA
final-demand code directly to ``SectorConsumedBy`` via a
``clean_fbs_after_aggregation`` hook
(``bedrock.transform.flowbyclean.assign_sector_consumed_by_from_clean_parameter``,
issue #539), so a plain ``generateFlowBySector`` call is enough here - no
per-activity_set replication needed.

``Cornerstone_2025_target.yaml``'s ``industry_spec`` (``default: NAICS_3``)
collapses most commodities not explicitly listed in its NAICS_4/5/6 override
lists down to a bare 3-digit NAICS parent that isn't a real BEA_2017_Detail
code (e.g. ``1112``, ``236`` - confirmed against ``USA_2017_COMMODITY_CODES``:
77% of rows / 86% of dollar value in the 2017 output). Other Cornerstone
models hit the same thing and fix it the same way: generate the FBS, then
call ``map_fbs_sectors_to_model_schema`` (``bedrock/transform/allocation/
derived.py``, e.g. the GHG pipeline's ``derived.py:404``) to expand+reallocate
each collapsed code back to a real NAICS_6/Cornerstone activity code. That
function is hardcoded to operate on ``SectorProducedBy``; ``SectorConsumedBy``
is resolved too via a temporary column swap, so both sides go through the
same correction consistently (a no-op for the final-demand codes there, since
they're not in the NAICS/Cornerstone crosswalks it consults).

``NIPA_final_dom_uses_<year>`` methods include ``BEA_detail_commodity_target.yaml``.
Trade FBS ``SectorProducedBy`` is BEA 2017 Detail (same include on
``Trade_Exports_<year>`` / ``Trade_Imports_<year>``). Overlay ``F04000``
after the NIPA frame exists; do not send Trade through
``map_fbs_sectors_to_model_schema``. ``S00900`` / ``F04000`` is the rest-of-world
identity ``-Y[S00900, F01000] + Supply_T016[S00900]`` (2017 only).
"""

from __future__ import annotations

import functools
import typing as ta

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.transform.iot.nowcast_intermediate import (
    DEFAULT_THETA,
    derive_intermediate_use,
)
from bedrock.transform.iot.nowcast_product_taxes import TOP_YEARS, top_column
from bedrock.transform.iot.nowcast_subsidies import SUB_YEARS, sub_column
from bedrock.transform.iot.nowcast_trade_margins import (
    TRADE_MARGIN_YEARS,
    trade_margin_column,
)
from bedrock.transform.iot.nowcast_transport_margins import transport_margin_column
from bedrock.transform.trade.duties import mdty_detail_usd
from bedrock.transform.trade.madj import madj_detail_usd
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: Years the transport margin can be built for. Truck and pipeline come from
#: the Service Annual Survey, which stops at 2022; AIES carries 2023 and is
#: not wired up, and 2024 is unpublished. Rail alone reaches 2024.
TRANSPORT_MARGIN_YEARS = range(2017, 2023)

# Same 12 codes as analysis ``SUPPLY_BRIDGE_CODES``. Kept here so nowcast does
# not import sections (sections already lazy-imports this module).
_SUPPLY_BRIDGE_CODES = (
    'T007',
    'MCIF',
    'MADJ',
    'T013',
    'TRADE',
    'TRANS',
    'T014',
    'MDTY',
    'TOP',
    'SUB',
    'T015',
    'T016',
)

#: Supply-bridge subtotals, each the plain signed sum of its components, in the
#: order they have to be evaluated (``T016`` consumes the other three). Verified
#: against the published 2017 Detail Supply table: every commodity reproduces to
#: within the workbook's own 1 million USD rounding (2 on ``T016``, which stacks
#: three rounded subtotals).
#:
#: ``SUB`` is stored **negative**, as BEA publishes it in the Supply table, so
#: ``T015`` adds it rather than subtracting.
_SUPPLY_BRIDGE_SUBTOTALS: dict[str, tuple[str, ...]] = {
    'T013': ('T007', 'MCIF', 'MADJ'),
    'T014': ('TRADE', 'TRANS'),
    'T015': ('MDTY', 'TOP', 'SUB'),
    'T016': ('T013', 'T014', 'T015'),
}

#: Value-added rows of the Use panel, same five as ``nowcast_mask.VA_ROWS``.
USE_VALUE_ADDED_ROWS = ('V00100', 'T00OTOP', 'V00300', 'T00TOP', 'T00SUB')

#: Value-added subtotal rows of the Use panel, as ``{row: {component: sign}}``,
#: in evaluation order. ``T005`` is the intermediate-use column total and is
#: derived from the commodity rows rather than read off a component. Verified
#: against the published 2017 Detail Use_SUT table: every industry reproduces to
#: within the workbook's rounding (1-2 million USD on the value-added rows, 13
#: on ``T005``/``T018``, which sum 402 rounded commodity cells).
#:
#: ⚠️ Sign convention is the balance's, not BEA's: ``T00SUB`` is stored
#: **negative** here (as ``nowcast_mask.published_2017_panel`` stores it, and as
#: the Supply table publishes ``SUB``), so ``VAPRO`` adds it. BEA publishes the
#: Use row positive and subtracts it. Feeding a BEA-signed panel in silently
#: doubles the subsidy wedge in ``VAPRO``.
_USE_VALUE_ADDED_SUBTOTALS: dict[str, dict[str, int]] = {
    'VABAS': {'V00100': 1, 'T00OTOP': 1, 'V00300': 1},
    'T018': {'T005': 1, 'VABAS': 1},
    'VAPRO': {'VABAS': 1, 'T00TOP': 1, 'T00SUB': 1},
}


def _signed_sum(parts: ta.Iterable[pd.Series], signs: ta.Iterable[int]) -> pd.Series:
    """Signed sum of series, propagating NaN.

    ``DataFrame.sum`` skips NaN, which would report an unsourced component as
    zero; a subtotal one of whose components is unsourced has to stay NaN, so
    the terms are added with ``+`` instead.
    """
    total: pd.Series | None = None
    for part, sign in zip(parts, signs, strict=True):
        term = part * sign
        total = term if total is None else total + term
    assert total is not None, 'a subtotal needs at least one component'
    return total


def fill_supply_bridge_subtotals(bridge: pd.DataFrame) -> pd.DataFrame:
    """Fill ``T013``/``T014``/``T015``/``T016`` from their components.

    Returns a new frame; the argument is not mutated. A subtotal whose
    components are not all sourced stays NaN - ``T014`` before the margin
    columns land, for instance - rather than being reported as zero.
    """
    filled = bridge.copy()
    for code, components in _SUPPLY_BRIDGE_SUBTOTALS.items():
        filled[code] = _signed_sum(
            (filled[c] for c in components), (1,) * len(components)
        )
    return filled


def use_value_added_subtotals(
    panel: pd.DataFrame, industries: ta.Sequence[str]
) -> pd.DataFrame:
    """``T005``/``VABAS``/``T018``/``VAPRO`` for the Use panel, industries only.

    ``panel`` is a Use block: commodity rows plus :data:`USE_VALUE_ADDED_ROWS`,
    on the balance's sign convention (see
    :data:`_USE_VALUE_ADDED_SUBTOTALS`). ``T005`` is the sum of the commodity
    rows, so the panel must carry them.

    Returned over ``industries`` only, matching the published table: BEA leaves
    the value-added subtotals blank in the final-demand columns, and ``T018``
    in particular is *not* ``T005 + VABAS`` there - it is empty, even though
    ``T005`` is not.
    """
    industries = list(industries)
    missing = [c for c in USE_VALUE_ADDED_ROWS if c not in panel.index]
    assert not missing, f'Use panel is missing value-added rows: {missing}'
    derived = {*USE_VALUE_ADDED_ROWS, *_USE_VALUE_ADDED_SUBTOTALS, 'T005'}
    commodities = [c for c in panel.index if c not in derived]
    assert commodities, 'Use panel has no commodity rows to total into T005'

    rows = panel.loc[list(USE_VALUE_ADDED_ROWS), industries].copy()
    rows.loc['T005'] = panel.loc[commodities, industries].sum()
    for code, terms in _USE_VALUE_ADDED_SUBTOTALS.items():
        rows.loc[code] = _signed_sum(
            (ta.cast('pd.Series', rows.loc[c]) for c in terms),
            tuple(terms.values()),
        )
    return rows.loc[['T005', *_USE_VALUE_ADDED_SUBTOTALS]]


_SECTOR_SWAP = {
    'SectorProducedBy': 'SectorConsumedBy',
    'SectorConsumedBy': 'SectorProducedBy',
}


def _resolve_both_sector_columns(fbs: pd.DataFrame) -> pd.DataFrame:
    """Apply ``map_fbs_sectors_to_model_schema`` to both SectorProducedBy and
    SectorConsumedBy (via a temporary swap - see module docstring)."""
    fbs = map_fbs_sectors_to_model_schema(fbs.rename(columns=_SECTOR_SWAP)).rename(
        columns=_SECTOR_SWAP
    )
    return map_fbs_sectors_to_model_schema(fbs)


def _trade_fbs_commodity_vector(method: str, download_sources_ok: bool) -> pd.Series:
    """Commodity totals from a Trade FBS, USD, indexed by BEA 2017 Detail.

    Groupby and reindex onto ``USA_2017_COMMODITY_CODES`` so industry-only
    Crosswalk rows (e.g. ``331314``) are dropped. Callers that overwrite
    ``S00900`` / ``F04000`` must do so after this vector is written. Mapped
    Detail mass only; ITA G+S scale is ``bedrock.transform.trade.scale``
    (#647).
    """
    fbs = getFlowBySector(
        method,
        download_FBAs_if_missing=download_sources_ok,
        download_FBS_if_missing=download_sources_ok,
    )
    return (
        pd.DataFrame(fbs)
        .groupby('SectorProducedBy')['FlowAmount']
        .sum()
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )


def _inventories_fbs_commodity_vector(
    method: str, download_sources_ok: bool
) -> pd.Series:
    """Commodity totals from the Inventories FBS, USD, indexed by BEA 2017 Detail.

    ``F03000`` is the one final-use column whose total is free: it equals NIPA
    CIPI exactly, 32,674 against the published column's 32,682. Everything else
    about it is allocation, and gross mass is 3x net - 98,764 against 32,682,
    with 61 negative commodities - so a column-total check is close to
    uninformative here. **Validate per commodity** (#529, #587).

    ⚠️ Mining and farm are still equal-split placeholders pending #660, so the
    per-commodity picture is not yet meaningful for those two branches even
    though the column total is.
    """
    fbs = getFlowBySector(
        method,
        download_FBAs_if_missing=download_sources_ok,
        download_FBS_if_missing=download_sources_ok,
    )
    return (
        pd.DataFrame(fbs)
        .groupby('SectorProducedBy')['FlowAmount']
        .sum()
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )


def _supply_fbs_commodity_vector(year: int, download_sources_ok: bool) -> pd.Series:
    """Domestic commodity output ``T007``, USD, indexed by BEA 2017 Detail.

    Source is the ``Detail_Supply_<year>`` FBS
    (``bedrock/transform/detail/Detail_Supply_<year>.yaml``), which
    disaggregates the published summary Supply domestic-output block onto the
    2017 detail mix. That block is commodity x industry, so ``T007`` is its
    **row margin** - and in the FBS the commodity is ``SectorConsumedBy``, the
    industry ``SectorProducedBy`` (the Supply table's rows are commodities and
    its columns industries).

    The 2017 build reproduces the published detail ``T007`` column to rounding:
    33,772,550m against 33,772,566m, worst commodity 8.6m on ``541511``'s
    269,868m. Later years close on the published *summary* row margin exactly
    by construction - the summary total is the control - so that agreement is
    not evidence; the detail split rests on the held-out mix test.

    ``S00300``, ``S00402`` and ``4200ID`` are absent because their published
    ``T007`` is zero by definition: they are not domestic output and enter the
    Supply table through ``MCIF`` / ``MDTY`` / margins instead. They reindex to
    0.0 here, which is their correct value, not a gap.
    """
    fbs = getFlowBySector(
        f'Detail_Supply_{year}',
        download_FBAs_if_missing=download_sources_ok,
        download_FBS_if_missing=download_sources_ok,
    )
    return (
        pd.DataFrame(fbs)
        .groupby('SectorConsumedBy')['FlowAmount']
        .sum()
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )


def _s00900_export_identity_usd() -> float:
    """2017 Supply T016 on S00900, scaled to USD (workbook is million USD)."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    supply.columns = supply.columns.str.strip()
    t016_m = pd.to_numeric(supply.loc['S00900', 'T016'], errors='raise')
    return float(t016_m) * MILLION_CURRENCY_TO_CURRENCY


@functools.cache
def derive_initial_Y_pur(year: int, download_sources_ok: bool = False) -> pd.DataFrame:
    """
    Initial (pre-RAS-balanced) final-demand section of the Use table, purchaser
    price, commodity x SUT final-demand codes (no F05000).

    F03000 is Inventories_<year> for 2017 (#529); other years all-zero.
    F04000 is mapped Trade_Exports_<year> Detail mass for 2017; other years
    all-zero. S00900/F04000 uses the rest-of-world identity against Supply
    T016 (2017).
    """
    fbs = FlowBySector.generateFlowBySector(
        f'NIPA_final_dom_uses_{year}', download_sources_ok=download_sources_ok
    )
    resolved = _resolve_both_sector_columns(pd.DataFrame(fbs))
    y = (
        resolved.groupby(['SectorProducedBy', 'SectorConsumedBy'])['FlowAmount']
        .sum()
        .unstack('SectorConsumedBy', fill_value=0)
        .reindex(columns=list(SUT_FINAL_DEMAND_CODES), fill_value=0.0)
        .sort_index()
    )
    y.index.name = 'commodity'
    y.columns.name = 'final_demand_code'

    # Every year is padded onto the full BEA detail commodity list, not just the
    # benchmark. A commodity absent from the groupby is one that receives no
    # final domestic use at all - in 2017 that is 81 of the 402, and they are
    # the same 81 that come back zero across all seventeen NIPA columns. Zero
    # and absent are the same fact here, but only one of them survives a join,
    # so the reindex has to happen before the frame is handed out (#621).
    y = y.reindex(y.index.union(USA_2017_COMMODITY_CODES), fill_value=0.0)
    if 'S00900' not in y.index:
        y.loc['S00900'] = 0.0

    if year == 2017:
        exports = _trade_fbs_commodity_vector(
            f'Trade_Exports_{year}', download_sources_ok
        )
        inventories = _inventories_fbs_commodity_vector(
            f'Inventories_{year}', download_sources_ok
        )
        y['F04000'] = exports.reindex(y.index).fillna(0.0)
        y['F03000'] = inventories.reindex(y.index).fillna(0.0)
        pce = float(pd.to_numeric(y.loc['S00900', 'F01000'], errors='raise'))
        y.loc['S00900', 'F04000'] = -pce + _s00900_export_identity_usd()

    y.index.name = 'commodity'
    return y.sort_index()


#: The three ``VABAS`` rows Step 2 builds from NIPA, one FBS method each, and
#: the Use row each writes.
#:
#: ⚠️ ``T00TOP``/``T00SUB`` are absent **from this dict**, and that is not the
#: same as absent from the block. They are built on the commodity axis in Step
#: 4d and converted to the industry axis by
#: :mod:`bedrock.analysis.nowcasting.tax_axis_conversion`, which measured the
#: conversion rather than assuming it: the Make matrix alone is useless (r =
#: 0.202, 114.6% error - it sends petroleum tax to refineries and motor-vehicle
#: tax to assemblers), but the producer/trade **level split** Step 4c already
#: computes, plus a handful of named routings, reaches **r = 0.948** at 27.9%
#: absolute error.
#:
#: So the older "their industry split is an output of Step 5's balance, not an
#: input to it" is **withdrawn**: 27.9% is not a target and the balance still
#: sets the final split under economy-wide soft targets, but a seed that good
#: is an input, and Step 5 gets it. Two pieces of it are exact rather than
#: estimated - customs duties are a lookup onto ``4200ID`` (38,513 against a
#: Supply ``MDTY`` of 38,510) and the ten government columns are zero by the
#: same accounting rule ``T00OTOP`` obeys.
_VALUE_ADDED_METHODS = {
    'V00100': 'NIPA_VA_compensation',
    'T00OTOP': 'NIPA_VA_othertax',
    'V00300': 'NIPA_VA_surplus',
}

#: The years all three methods have a file for. 2017 is the benchmark, and the
#: horizon is the nowcast's rather than any source's - NIPA and ``UVA205-A``
#: both run well past it.
VALUE_ADDED_YEARS = tuple(range(2017, 2025))


@functools.cache
def derive_initial_value_added(
    year: int, download_sources_ok: bool = False
) -> pd.DataFrame:
    """
    Initial (pre-balance) value-added block of the Use table, basic price,
    value-added code x industry, in USD (#538).

    Runs the three ``NIPA_VA_*_<year>`` methods and stacks them. Each writes its
    own row -- ``V00100`` compensation on 69 NIPA industry controls, ``T00OTOP``
    on one, ``V00300`` on eight across five tables -- with the row code on
    ``SectorProducedBy`` and the industry on ``SectorConsumedBy``, which is the
    transpose of :func:`derive_initial_Y_pur`'s orientation.

    ✅ **2017-2024, and the three rows are three different claims.** Reading
    the block as "value added, nowcast" overstates two thirds of it, so what
    each row is worth is worth keeping straight:

    ``V00100``
        An **estimate**. 2017 detail shares carried on QCEW payroll growth,
        renormalised inside each of ``T60200D``'s 69 NIPA industry groups, then
        rescaled to that group's published control - see
        :mod:`bedrock.transform.nipa.compensation_movement`, which took a
        ``clean_fba`` socket on the existing attribution source rather than the
        ``FBS_outside_flowsa`` hatch that blocked it (#731). Graded -10.0%
        against frozen shares on the observed 2012->2017 holdout.

    ``T00OTOP``
        A **level plus two lookups**. The ``T30500`` control is read per year
        (+40.5% over the span); the housing block is rescaled to ``T70405``
        ``B1031C`` and the farm block to ``T70305`` ``B1017C``, both published
        annually and both exact against the benchmark; the remaining 56.7%
        keeps 2017's within-block shape, which the held-out summary SUT
        licenses at 1.01-2.10% drift. So 43.3% of the row is observed rather
        than assumed. See :mod:`bedrock.transform.nipa.othertax_lookups` and
        :mod:`bedrock.analysis.nowcasting.other_taxes_allocation`.

    ``V00300``
        A **seed**, and only a seed. Level from the eight-line NIPA assembly
        per year, shares frozen at 2017 - where drift reaches **12.51%** by
        2022, six times ``T00OTOP``'s. That is acceptable only because T18
        changed what the row is: with ``VAPRO`` pinned per industry
        (:func:`~bedrock.transform.iot.nowcast_targets.industry_value_added_target`)
        gross operating surplus is the **residual the balance solves for**, and
        the closer overwrites this distribution. Do not "improve" it with
        ``TVA113`` - that is the grader, held out by Step 5's Decision 3.

    ⚠️ **The summary SUT is a stale grader for 2019-2022.** Its own ``VAPRO``
    total sits 0.09-1.21% below current-vintage ``UVA205-A`` in exactly those
    four years and matches to the dollar in 2017, 2018, 2023 and 2024. So an
    apparent 2.64% ``V00300`` assembly error in 2022 is the workbook being
    behind, not the assembly being wrong - and ``V00300`` shows it at roughly
    twice ``VAPRO``'s rate because it is the row a NIPA revision lands in.

    ⚠️ **Rows may be negative and must stay so.** ``S00201`` state and local
    passenger transit carries a ``V00300`` of -36,919 million in 2017 and stays
    negative in every year; it is the only industry that does.
    """
    if year not in VALUE_ADDED_YEARS:
        raise ValueError(
            f'the value-added block is built for '
            f'{min(VALUE_ADDED_YEARS)}-{max(VALUE_ADDED_YEARS)}; got {year}. '
            f'Each row needs a NIPA_VA_*_{year} method file, and the benchmark '
            f'attribution weights come from the 2017 detail Use SUT, which BEA '
            f'publishes for no other year.'
        )
    rows = []
    for code, method in _VALUE_ADDED_METHODS.items():
        fbs = FlowBySector.generateFlowBySector(
            f'{method}_{year}', download_sources_ok=download_sources_ok
        )
        resolved = _resolve_both_sector_columns(pd.DataFrame(fbs))
        by_industry = resolved.groupby('SectorConsumedBy')['FlowAmount'].sum()
        rows.append(by_industry.rename(code))
    block = pd.DataFrame(rows).reindex(columns=list(USA_2017_INDUSTRY_CODES))
    block = block.fillna(0.0).astype(float)
    block.index.name = 'value_added_code'
    block.columns.name = 'industry'
    return block


@functools.cache
def derive_initial_supply_bridge(
    year: int, download_sources_ok: bool = False
) -> pd.DataFrame:
    """Commodity x Supply-bridge codes, USD, BEA 2017 Detail rows.

    MCIF is mapped Trade_Imports_<year> Detail mass for 2017. MDTY is Census
    duty rate × goods MCIF, leveled to NIPA B235RC, for 2017. MADJ is Census
    GEN_CHA_YR reassigned onto 2017 Supply MADJ destination codes, leveled
    to published Supply MADJ, for 2017.

    ``T007`` is the row margin of the ``Detail_Supply_<year>`` FBS
    domestic-output block, and is sourced for every year **2017-2024** (#570).

    ``TRANS`` is Step 4c's transport margin, built per mode on the basis BEA
    uses for each and controlled to that mode's observed annual freight revenue
    (#611). Unlike the trade columns it is sourced for **2017-2022**, not 2017
    alone, because it never touches the nowcast base - each mode is allocated
    from its own revenue. It is the one margin column that does not wait on 4a.

    ⚠️ ``TRANS`` is filled with **zeros** for commodities that bear no transport
    margin, not NaN. A commodity outside the receiving set genuinely has no
    transport margin; that is sourced information, not an unfilled cell, and the
    column has to net to zero for target T16 to hold.

    ``TRADE`` is Step 4c's trade margin, anchored on the published 2017 give-up
    and moved by the Census wholesale and retail gross margin (#612, #613).
    Sourced for **2017-2023** - one year further than ``TRANS``, because the
    Census series runs to 2023 where SAS stops at 2022.

    ⚠️ **It does not wait on 4a or 4d after all.** The plan reached ``TRADE`` as
    a rate on producer value, which would have needed the nowcast base; the
    anchor-and-move construction reaches the same column from the give-up side
    instead, and the give-up is observed. Like ``TRANS`` it never touches the
    base, so it carries no circularity with Step 6b.

    ⚠️ Both margin columns are filled with **zeros** for commodities that bear no
    margin, not NaN - a commodity outside the receiving set genuinely has none,
    and the column has to net to zero for target T16 to hold.

    ``TOP`` is Step 4d's taxes-on-products column (#580), sourced for **all of
    2017-2024** because every input is NIPA. Its total is observed rather than
    estimated - NIPA T30500 taxes on products less customs duties, 716,925
    against the published 716,926 in 2017 - and 29.8% of the commodity split
    comes from NIPA's own named product lines, which move very differently from
    the column (tobacco falls 40% by 2024 while the column rises 42%). The
    remaining 70% is general sales tax on frozen 2017 shares.

    ``SUB`` is Step 4d's subsidy column (#580), sourced for **all of 2017-2024**.
    Its total is NIPA T31300, observed. Each commodity is anchored on its
    published 2017 value and moved by its own NIPA type line, except that 2020
    and 2021 replace the ``other`` type - 84% of the column in those years - with
    BEA's published allocation of PPP across industries, because the 2017 vector
    is 64% insurance carriers and moving it would put ~377bn of pandemic support
    there.

    ⚠️ ``SUB`` is stored **negative**, BEA's Supply-table convention, and ``T015``
    adds it rather than subtracting. The Use table's ``T00SUB`` row carries the
    same money positive.

    The subtotals
    T013/T014/T015/T016 are computed from their components by
    :func:`fill_supply_bridge_subtotals`, so a subtotal is NaN until every one
    of its components is sourced. Callers must not mutate the cached frame.
    """
    bridge = pd.DataFrame(
        index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'),
        columns=list(_SUPPLY_BRIDGE_CODES),
        dtype=float,
    )
    bridge.columns.name = 'supply_bridge_code'
    bridge['T007'] = _supply_fbs_commodity_vector(year, download_sources_ok)
    if year == 2017:
        bridge['MCIF'] = _trade_fbs_commodity_vector(
            f'Trade_Imports_{year}', download_sources_ok
        )
        bridge['MDTY'] = mdty_detail_usd(year, download_sources_ok)
        bridge['MADJ'] = madj_detail_usd(year, download_sources_ok)
    if year in TRANSPORT_MARGIN_YEARS:
        bridge['TRANS'] = (
            transport_margin_column(year).reindex(bridge.index).fillna(0.0)
        )
    if year in TRADE_MARGIN_YEARS:
        bridge['TRADE'] = trade_margin_column(year).reindex(bridge.index).fillna(0.0)
    if year in TOP_YEARS:
        bridge['TOP'] = top_column(year).reindex(bridge.index).fillna(0.0)
    if year in SUB_YEARS:
        bridge['SUB'] = sub_column(year).reindex(bridge.index).fillna(0.0)
    return fill_supply_bridge_subtotals(bridge)


def derive_initial_U_intermediate(
    year: int, theta: float = DEFAULT_THETA
) -> pd.DataFrame:
    """Initial (pre-RAS-balanced) intermediate block of the Use table, USD.

    Commodity x industry, 402 x 402, **purchaser** price and **before**
    redefinitions - the native basis of ``Use_SUT_Framework_2017_DET``, which is
    what this is seeded from.

    Step 3 (#497). The sourcing, the carry, the column control and every caveat
    live in :mod:`bedrock.transform.iot.nowcast_intermediate`; read that module
    docstring before using this for anything but the section diagnostic. In
    particular ``theta`` defaults to #497's 1.0 and **fits negative at 2023-24**,
    and the column control is a *seed* because Step 2 is unbuilt.
    """
    return derive_intermediate_use(year, theta=theta)
