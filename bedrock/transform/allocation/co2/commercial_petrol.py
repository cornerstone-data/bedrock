from __future__ import annotations

import pandas as pd

from bedrock.transform.allocation.co2.commercial_coal import ALLOCATION_SECTORS
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import load_mmt_co2e_across_fuel_types


def allocate_commercial_petrol() -> pd.Series[float]:
    emissions = load_mmt_co2e_across_fuel_types().loc["Total Petroleum", "Comm"]
    assert isinstance(emissions, float)

    pct = (
        load_bea_use_table().loc[pd.Index(ALLOCATION_SECTORS), "324110"].astype(float)
    )  # Petroleum refineries
    pct = pct / pct.sum()

    allocated = emissions * pct
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
