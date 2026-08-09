"""Nowcasted US national Make/Use/Import tables.

Goal: build a full national Make, Use, and Import table for a series of years,
assembled from independently-sourced sections, ultimately converted to the
Cornerstone schema (after redefinitions) and RAS-rebalanced against known
controls. See https://github.com/orgs/cornerstone-data/projects/26 for the tracking board.

This module implements the final-demand section of the Use table, in
purchaser (PUR) price, commodity x SUT final-demand codes (MUT list minus
``F05000``). Source: the ``NIPA_FD_<year>`` FBS methods
(``bedrock/transform/nipa/NIPA_FD_<year>.yaml``) plus, for 2017,
``Trade_Exports_<year>`` on ``F04000``. ``F03000`` (change in private
inventories, #529) is present but all-zero. ``F05000`` is MUT-only and is
not a Y column.

Each ``NIPA_FD_<year>.yaml`` activity_set assigns its official BEA
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

Trade FBS ``SectorProducedBy`` is already BEA 2017 Detail. Overlay ``F04000``
after the NIPA frame exists; do not send Trade through
``map_fbs_sectors_to_model_schema``. ``S00900`` / ``F04000`` is the rest-of-world
identity ``-Y[S00900, F01000] + Supply_T016[S00900]`` (2017 only).
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.flowbysector import FlowBySector
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES

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
    """Commodity totals from a Trade FBS, USD, indexed by BEA 2017 Detail."""
    fbs = FlowBySector.generateFlowBySector(
        method, download_sources_ok=download_sources_ok
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

    F03000 (change in private inventories, #529) is present but all-zero.
    F04000 is Trade_Exports_<year> for 2017; other years all-zero.
    S00900/F04000 uses the rest-of-world identity against Supply T016 (2017).
    """
    fbs = FlowBySector.generateFlowBySector(
        f'NIPA_FD_{year}', download_sources_ok=download_sources_ok
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
        y = y.reindex(y.index.union(USA_2017_COMMODITY_CODES), fill_value=0.0)
        y['F04000'] = exports.reindex(y.index).fillna(0.0)
        if 'S00900' not in y.index:
            y.loc['S00900'] = 0.0
        y.loc['S00900', 'F04000'] = (
            -y.loc['S00900', 'F01000'] + _s00900_export_identity_usd()
        )

    y.index.name = 'commodity'
    return y.sort_index()
