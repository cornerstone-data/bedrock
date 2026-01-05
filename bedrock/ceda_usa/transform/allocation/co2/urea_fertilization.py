from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)


def allocate_urea_fertilization() -> pd.Series[float]:
    allocation_sectors = [
        "1111A0",
        "1111B0",
        "111200",
        "111300",
        "111400",
        "111900",
        "112120",
    ]
    pct = load_bea_use_table().loc[
        pd.Index(allocation_sectors), "325310"
    ]  # fertilizer manufacturing
    pct = pct / pct.sum()
    emissions = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("CO2", "Urea Fertilization")
    ]
    allocated = emissions * pct
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
