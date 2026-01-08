from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_ch4_and_n2o_emissions_from_manure_management,
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

ALLOCATION: ta.Dict[ta.Tuple[str, str], str] = {
    ("N2Ob", "Dairy Cattle"): "112120",
    ("N2Ob", "Beef Cattle"): "1121A0",
    ("N2Ob", "Swine"): "112A00",
    ("N2Ob", "Sheep"): "112A00",
    ("N2Ob", "Goats"): "112A00",
    ("N2Ob", "Poultry"): "112300",
    ("N2Ob", "Horses"): "112A00",
}


def allocate_manure_management() -> pd.Series[float]:
    emissions = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("N2Oc", "Manure Management")
    ]
    tbl_5_7 = load_ch4_and_n2o_emissions_from_manure_management()
    pct = tbl_5_7.groupby(ALLOCATION).sum()  # type:ignore
    pct = pct / pct.sum()

    return (pct * emissions).reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
