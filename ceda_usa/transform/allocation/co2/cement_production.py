from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_cement_production() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "327310": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("CO2", "Cement Production")
            ]
        }
    )

    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
