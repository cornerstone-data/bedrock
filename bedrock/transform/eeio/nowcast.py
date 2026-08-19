"""Nowcasted US national Make/Use/Import tables.

Goal: build a full national Make, Use, and Import table for a series of years,
assembled from independently-sourced sections, ultimately converted to the
Cornerstone schema (after redefinitions) and RAS-rebalanced against known
controls. See https://github.com/orgs/cornerstone-data/projects/26 for the tracking board.

This module implements the final-demand section of the Use table, in
purchaser (PUR) price, commodity x SUT final-demand codes (MUT list minus
``F05000``), and the Supply bridge block (commodity x the 12 basic-to-
purchaser codes). Y source: the ``NIPA_final_dom_uses_<year>`` FBS methods
(``bedrock/transform/nipa/NIPA_final_dom_uses_<year>.yaml``) plus, for 2017,
``Trade_Exports_<year>`` on ``F04000`` via mapped Detail mass in
``_trade_fbs_commodity_vector``. ``F03000`` (change in private
inventories, #529) is present but all-zero. ``F05000`` is MUT-only and
is not a Y column. The Supply bridge fills ``MCIF`` from
``Trade_Imports_<year>`` (same mapped Detail mass), ``MDTY`` from Census
duty rates leveled to NIPA ``B235RC``, and ``MADJ`` from Census import
charges (``GEN_CHA_YR``) reassigned onto 2017 Supply ``MADJ``
destination codes and leveled to published Supply ``MADJ``, for 2017;
other bridge columns are unsourced. ITA G+S scale lives in
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

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
from bedrock.transform.iot.nowcast_transport_margins import transport_margin_column
from bedrock.transform.trade.duties import mdty_detail_usd
from bedrock.transform.trade.madj import madj_detail_usd
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES

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

    if year == 2017:
        exports = _trade_fbs_commodity_vector(
            f'Trade_Exports_{year}', download_sources_ok
        )
        inventories = _inventories_fbs_commodity_vector(
            f'Inventories_{year}', download_sources_ok
        )
        y = y.reindex(y.index.union(USA_2017_COMMODITY_CODES), fill_value=0.0)
        y['F04000'] = exports.reindex(y.index).fillna(0.0)
        y['F03000'] = inventories.reindex(y.index).fillna(0.0)
        if 'S00900' not in y.index:
            y.loc['S00900'] = 0.0
        pce = float(pd.to_numeric(y.loc['S00900', 'F01000'], errors='raise'))
        y.loc['S00900', 'F04000'] = -pce + _s00900_export_identity_usd()

    y.index.name = 'commodity'
    return y.sort_index()


@functools.cache
def derive_initial_supply_bridge(
    year: int, download_sources_ok: bool = False
) -> pd.DataFrame:
    """Commodity x Supply-bridge codes, USD, BEA 2017 Detail rows.

    MCIF is mapped Trade_Imports_<year> Detail mass for 2017. MDTY is Census
    duty rate × goods MCIF, leveled to NIPA B235RC, for 2017. MADJ is Census
    GEN_CHA_YR reassigned onto 2017 Supply MADJ destination codes, leveled
    to published Supply MADJ.

    ``TRANS`` is Step 4c's transport margin, built per mode on the basis BEA
    uses for each and controlled to that mode's observed annual freight revenue
    (#611). Unlike the trade columns it is sourced for **2017-2022**, not 2017
    alone, because it never touches the nowcast base - each mode is allocated
    from its own revenue. It is the one margin column that does not wait on 4a.

    ⚠️ ``TRANS`` is filled with **zeros** for commodities that bear no transport
    margin, not NaN. A commodity outside the receiving set genuinely has no
    transport margin; that is sourced information, not an unfilled cell, and the
    column has to net to zero for target T16 to hold.

    ``TRADE`` remains unsourced: it is a rate on producer value, so it needs
    4a (#570) and 4d (#580) first. Other years and remaining columns (T007,
    tax, subtotals, T013) are unsourced. Callers must not mutate the cached
    frame.
    """
    bridge = pd.DataFrame(
        index=pd.Index(USA_2017_COMMODITY_CODES, name='commodity'),
        columns=list(_SUPPLY_BRIDGE_CODES),
        dtype=float,
    )
    bridge.columns.name = 'supply_bridge_code'
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
    return bridge
