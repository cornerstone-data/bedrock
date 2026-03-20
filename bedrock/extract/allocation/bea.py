from __future__ import annotations

import functools
import os
import posixpath
from collections.abc import Sequence

import pandas as pd

from bedrock.transform.eeio.derived_2017 import derive_2017_V_usa
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import GCS_CEDA_INPUT_DIR
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

# Schema alignment: CEDA allocator sectors → Cornerstone use table.
_WASTE_AGGREGATE = "562000"
_WASTE_SUBS = list(WASTE_DISAGG_COMMODITIES[_WASTE_AGGREGATE])
_APPLIANCE_AGGREGATE = "335220"
_APPLIANCE_SUBS = ["335221", "335222", "335224", "335228"]
# CEDA 331313 = primary + secondary aluminum; Cornerstone has 331313 + 33131B.
_CEDA_331313_CORNERSTONE_PARTS = ("331313", "33131B")

GCS_BEA_PCE_DIR = posixpath.join(
    GCS_CEDA_INPUT_DIR, "BEA_PersonalConsumptionExpenditure"
)
IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


@functools.cache
def load_bea_make_table() -> pd.DataFrame:
    """
    This is a wrapper function that loads the latest BEA Supply table.
    WARNING: this table is 2017 Supply and is transposed from the 2012 Make,
    so the rows are the industries and the columns are the commodities
    """

    bea_make = derive_2017_V_usa()
    assert (
        bea_make.index == CEDA_V7_SECTORS
    ).any(), "BEA make table has incorrect index."
    assert (
        bea_make.columns == CEDA_V7_SECTORS
    ).any(), "BEA make table has incorrect columns."
    return bea_make


@functools.cache
def load_bea_use_table() -> pd.DataFrame:
    """Load BEA Use and Final Demand tables in Cornerstone schema.

    Rows = industries + one PCE row; columns = commodities. Result is cached.
    """
    from bedrock.transform.eeio import derived_cornerstone

    uset = derived_cornerstone.derive_cornerstone_U_set()
    U_combined = (uset.Udom + uset.Uimp).T
    Y_cs = (
        derived_cornerstone.derive_cornerstone_Y_personal_consumption_expenditure()
        .to_frame()
        .T
    )
    return pd.concat([U_combined, Y_cs])


def _use_table_value_ceda_sector_cornerstone_aligned(
    col: pd.Series,
    table_idx: pd.Index,
    ceda_sector: str,
) -> float:
    """
    Value for one CEDA allocator sector from a use table (CEDA or Cornerstone shaped).

    When the table is CEDA-shaped (Cornerstone schema not active), the sector is
    in the index and we return it directly; alignment rules are skipped. When the
    table is Cornerstone-shaped, we apply alignment: 562* → 562000, 335220 ↔ 4
    appliance sectors, 331313 → 331313+33131B.
    """
    if ceda_sector in table_idx:
        return float(col.loc[ceda_sector])
    # 562000: consolidate waste subsectors (table has 562111, 562HAZ, ...)
    if ceda_sector == _WASTE_AGGREGATE:
        present = table_idx.intersection(pd.Index(_WASTE_SUBS))
        if len(present) > 0:
            return float(col.loc[present].sum())
        return 0.0
    # 335220: consolidate appliance subsectors (table has 335221, 335222, ...)
    if ceda_sector == _APPLIANCE_AGGREGATE:
        present = table_idx.intersection(pd.Index(_APPLIANCE_SUBS))
        if len(present) > 0:
            return float(col.loc[present].sum())
        return 0.0
    # 335221/335222/335224/335228: split 335220 equally (table has aggregate only)
    if ceda_sector in _APPLIANCE_SUBS and _APPLIANCE_AGGREGATE in table_idx:
        return float(col.loc[_APPLIANCE_AGGREGATE]) / len(_APPLIANCE_SUBS)
    # 331313 (CEDA aggregate): sum Cornerstone 331313 + 33131B when present
    if ceda_sector == "331313":
        parts = [p for p in _CEDA_331313_CORNERSTONE_PARTS if p in table_idx]
        if parts:
            return float(col.loc[parts].sum())
        return 0.0
    return 0.0


def use_table_series_ceda_allocator_to_cornerstone_schema(
    use_table: pd.DataFrame,
    ceda_allocator_sectors: Sequence[str],
    commodity: str,
) -> pd.Series:
    """
    Use-table series for CEDA allocator sectors, aligned to Cornerstone schema.

    When the use table is CEDA-shaped (Cornerstone schema not active), sectors
    are looked up directly; alignment is skipped. When Cornerstone-shaped,
    alignment applies: 562* → 562000, 335220 ↔ 4 appliance sectors, 331313 →
    331313+33131B. Missing sectors get 0. Safe to normalize (e.g. pct = s / s.sum()).
    """
    table_idx = use_table.index
    col = use_table[commodity].astype(float)
    values = [
        _use_table_value_ceda_sector_cornerstone_aligned(col, table_idx, s)
        for s in ceda_allocator_sectors
    ]
    return pd.Series(values, index=pd.Index(ceda_allocator_sectors))


@functools.cache
def load_bea_personal_consumption_expenditure() -> pd.Series[float]:
    """
    Latest BEA Personal Consumption Expenditure by Major Type of Product from
    https://apps.bea.gov/iTable/?reqid=19&step=2&isuri=1&categories=survey&_gl=1*1mu0824*_ga*MTkyNDEyMDE5LjE3MTA0NjE1MjE.*_ga_J4698JNNFT*MTcxMDQ2MTUyMC4xLjEuMTcxMDQ2MjIyNS4xNC4wLjA.#eyJhcHBpZCI6MTksInN0ZXBzIjpbMSwyLDMsM10sImRhdGEiOltbImNhdGVnb3JpZXMiLCJTdXJ2ZXkiXSxbIk5JUEFfVGFibGVfTGlzdCIsIjY1Il0sWyJGaXJzdF9ZZWFyIiwiMjAxMiJdLFsiTGFzdF9ZZWFyIiwiMjAyMyJdLFsiU2NhbGUiLCItNiJdLFsiU2VyaWVzIiwiQSJdXX0=
    """
    tbl = load_from_gcs(
        name="BEA Personal Consumption Expenditures by Major Type of Product_June27_2024.csv",
        sub_bucket=GCS_BEA_PCE_DIR,
        local_dir=IN_DIR,
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=3,
            index_col=1,
        )
        .dropna()
        .drop(columns=["Line"]),
    )
    tbl.index = tbl.index.str.strip()
    tbl.columns = tbl.columns.astype(int)
    return tbl.loc[:, get_usa_config().usa_ghg_data_year]
