from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)


def allocate_nf3_semiconductor_manufacture() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "334413": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                (
                    "NF3",
                    "Electronics Industry",  # formerly "Semiconductor Manufacturing"
                ),
            ]
        }
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
