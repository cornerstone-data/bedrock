from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.transform.allocation.waste_utils import (
    waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema,
)


def allocate_landfills() -> pd.Series[float]:
    total = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("CH4c", "Landfills")
    ]
    allocated = waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema(
        float(total)
    )
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
