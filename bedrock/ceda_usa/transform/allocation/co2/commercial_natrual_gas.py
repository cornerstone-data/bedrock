from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import load_mmt_co2e_across_fuel_types
from bedrock.ceda_usa.transform.allocation.co2.commercial_coal import ALLOCATION_SECTORS
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_commercial_natural_gas() -> pd.Series[float]:
    emissions = load_mmt_co2e_across_fuel_types().loc["Natural Gas", "Comm"]
    assert isinstance(emissions, float)

    pct = (
        load_bea_use_table().loc[pd.Index(ALLOCATION_SECTORS), "221200"].astype(float)
    )  # Natural gas distribution
    pct = pct / pct.sum()

    allocated = emissions * pct
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
