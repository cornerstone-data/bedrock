from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)


def allocate_nitric_acid() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "325310": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("N2Oc", "Nitric Acid Production")
            ]
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
