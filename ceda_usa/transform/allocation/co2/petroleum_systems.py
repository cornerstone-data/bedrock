from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import load_co2_emissions_from_petroleum_systems
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_petroleum_systems() -> pd.Series[float]:
    ser = load_co2_emissions_from_petroleum_systems()
    allocated = pd.Series(
        {
            "211000": ser["Exploration"] + ser["Production "],
            "324110": ser["Crude Refining"],
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
