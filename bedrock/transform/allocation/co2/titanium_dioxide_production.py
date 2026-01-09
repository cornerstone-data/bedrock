from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_titanium_dioxide_production() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "325190": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("CO2", "Titanium Dioxide Production")
            ]
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
