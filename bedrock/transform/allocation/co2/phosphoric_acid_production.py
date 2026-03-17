from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_phosphoric_acid_production() -> pd.Series[float]:
    allocated = pd.Series(
        {
            "325310": load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("CO2", "Phosphoric Acid Production")
            ]
        }
    )
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
