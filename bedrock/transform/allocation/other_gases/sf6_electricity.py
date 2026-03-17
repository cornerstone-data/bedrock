from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_sf6_electricity() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "221100": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                # Note: 2022 data (2024 report) renames "Electrical Transmission and Distribution" to "Electrical Equipment"
                ("SF6", "Electrical Equipment")
            ]
        }
    )
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
