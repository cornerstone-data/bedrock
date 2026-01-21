from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_stationary_combustion,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_stationary_combustion_electric() -> pd.Series[float]:
    total_emissions = load_ch4_emissions_from_stationary_combustion().loc[
        ("Electric Power", "TOTAL")
    ]
    allocated_vec = pd.Series(0.0, index=CEDA_V7_SECTORS)
    allocated_vec["221100"] = total_emissions
    return allocated_vec * MEGATONNE_TO_KG
