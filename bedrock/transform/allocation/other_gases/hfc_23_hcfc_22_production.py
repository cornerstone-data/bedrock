from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_hfc_23_hcfc_22_production() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "325120": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                # Note: 2022 data (2024 report) includes fluorinated gases from fluorochemical production
                # other than HCFC-22 within the fluorochemical production category.
                # This was "HCFC-22 Production"
                ("HFCs", "Fluorochemical Production")
            ]
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
