from __future__ import annotations

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)


def allocate_incineration_of_waste() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "562000": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("N2Oc", "Incineration of Waste")
            ]
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
