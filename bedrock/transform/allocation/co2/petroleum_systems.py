from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_petroleum_systems,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_petroleum_systems() -> pd.Series[float]:
    ser = load_co2_emissions_from_petroleum_systems()
    allocated = pd.Series(
        {
            "211000": ser["Exploration"] + ser["Production "],
            "324110": ser["Crude Refining"],
        }
    )
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
