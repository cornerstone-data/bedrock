"""Nowcasted US national Make/Use/Import tables.

Goal: build a full national Make, Use, and Import table for a series of years,
assembled from independently-sourced sections, ultimately converted to the
Cornerstone schema (after redefinitions) and RAS-rebalanced against known
controls. See https://github.com/orgs/cornerstone-data/projects/26 for the tracking board.

This module currently implements only the final-demand section of the
Use table, in purchaser (PUR) price, shaped like BEA's ``BEA_2017_Detail``
schema (commodity x ``BEA_2017_FINAL_DEMAND_CODE``). Source: the
``NIPA_FD_<year>`` FBS methods (``bedrock/transform/nipa/NIPA_FD_<year>.yaml``).

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
"""

from __future__ import annotations

import functools
from collections.abc import Generator
from contextlib import contextmanager

import pandas as pd

from bedrock.transform.allocation.derived import map_fbs_sectors_to_model_schema
from bedrock.transform.flowbysector import FlowBySector
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.v2017_final_demand import BEA_2017_FINAL_DEMAND_CODES

_SECTOR_SWAP = {'SectorProducedBy': 'SectorConsumedBy', 'SectorConsumedBy': 'SectorProducedBy'}


@contextmanager
def _force_cornerstone_model_schema() -> Generator[None, None, None]:
    """
    ``map_fbs_sectors_to_model_schema`` branches on
    ``get_usa_config().use_cornerstone_2026_model_schema`` (default False) to
    pick the Cornerstone_2025 vs. CEDA_2025 crosswalk. Temporarily forces it
    True on the live global config (rather than swapping to a whole different
    named config via ``temp_usa_config``, which would also reset unrelated
    fields like ``model_base_year``) and restores the prior value after.
    """
    cfg = get_usa_config()
    previous = cfg.use_cornerstone_2026_model_schema
    cfg.use_cornerstone_2026_model_schema = True
    try:
        yield
    finally:
        cfg.use_cornerstone_2026_model_schema = previous


def _resolve_both_sector_columns(fbs: pd.DataFrame) -> pd.DataFrame:
    """Apply ``map_fbs_sectors_to_model_schema`` to both SectorProducedBy and
    SectorConsumedBy (via a temporary swap - see module docstring)."""
    with _force_cornerstone_model_schema():
        fbs = map_fbs_sectors_to_model_schema(fbs.rename(columns=_SECTOR_SWAP)).rename(
            columns=_SECTOR_SWAP
        )
        return map_fbs_sectors_to_model_schema(fbs)


@functools.cache
def derive_initial_Y_pur(year: int, download_sources_ok: bool = False) -> pd.DataFrame:
    """
    Initial (pre-RAS-balanced) final-demand section of the Use table, purchaser
    price, BEA_2017_Detail schema (commodity x BEA_2017_FINAL_DEMAND_CODE).

    Columns not yet sourced (F03000 change in private inventories, F04000
    exports, F05000 imports - see issues #529 and #526) are present but
    all-zero.
    """
    fbs = FlowBySector.generateFlowBySector(
        f'NIPA_FD_{year}', download_sources_ok=download_sources_ok
    )
    resolved = _resolve_both_sector_columns(pd.DataFrame(fbs))
    y = (
        resolved.groupby(['SectorProducedBy', 'SectorConsumedBy'])['FlowAmount']
        .sum()
        .unstack('SectorConsumedBy', fill_value=0)
        .reindex(columns=BEA_2017_FINAL_DEMAND_CODES, fill_value=0.0)
        .sort_index()
    )
    y.index.name = 'commodity'
    y.columns.name = 'final_demand_code'
    return y
