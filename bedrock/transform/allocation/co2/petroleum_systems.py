from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_petroleum_systems,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_petroleum_systems() -> pd.Series[float]:
    ser = load_co2_emissions_from_petroleum_systems()
    allocated = pd.Series(
        {
            "211000": ser["Exploration"] + ser["Production "],
            "324110": ser["Crude Refining"],
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
