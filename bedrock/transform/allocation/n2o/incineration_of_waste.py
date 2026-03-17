from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_incineration_of_waste() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "562000": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("N2Oc", "Incineration of Waste")
            ]
        }
    )
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
