from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_electric() -> pd.Series[float]:
    total_emissions = load_n2o_emissions_from_stationary_combustion().loc[
        ("Electric Power", "TOTAL")
    ]
    allocated_vec = pd.Series(0.0, index=CEDA_V7_SECTORS)
    allocated_vec["221100"] = total_emissions

    return allocated_vec * MEGATONNE_TO_KG
