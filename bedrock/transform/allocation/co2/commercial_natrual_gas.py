from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import (
    load_bea_use_table,
    use_table_series_ceda_allocator_to_cornerstone_schema,
)
from bedrock.extract.allocation.epa import load_mmt_co2e_across_fuel_types
from bedrock.transform.allocation.co2.commercial_coal import ALLOCATION_SECTORS
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_commercial_natural_gas() -> pd.Series[float]:
    emissions = load_mmt_co2e_across_fuel_types().loc["Natural Gas", "Comm"]
    assert isinstance(emissions, float)

    # CEDA allocator sectors aligned to Cornerstone schema when use table is Cornerstone.
    pct = use_table_series_ceda_allocator_to_cornerstone_schema(
        load_bea_use_table(), ALLOCATION_SECTORS, "221200"
    )  # Natural gas distribution
    pct = pct / pct.sum()

    allocated = emissions * pct
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
