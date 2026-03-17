from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import (
    load_bea_use_table,
    use_table_series_ceda_allocator_to_cornerstone_schema,
)
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_urea_fertilization() -> pd.Series[float]:
    allocation_sectors = [
        "1111A0",
        "1111B0",
        "111200",
        "111300",
        "111400",
        "111900",
        "112120",
    ]
    # CEDA allocator sectors aligned to Cornerstone schema when use table is Cornerstone.
    pct = use_table_series_ceda_allocator_to_cornerstone_schema(
        load_bea_use_table(), allocation_sectors, "325310"
    )  # fertilizer manufacturing
    pct = pct / pct.sum()
    emissions = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("CO2", "Urea Fertilization")
    ]
    allocated = emissions * pct
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
