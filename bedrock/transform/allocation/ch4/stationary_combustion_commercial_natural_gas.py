from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import (
    load_bea_use_table,
    use_table_series_ceda_allocator_to_cornerstone_schema,
)
from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_stationary_combustion,
)
from bedrock.transform.allocation.constants import (
    COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_commercial_natural_gas() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_stationary_combustion().loc[
        ("Commercial", "Natural gas")  # previously Institutional Natural gas
    ]
    # CEDA allocator sectors aligned to Cornerstone schema when use table is Cornerstone.
    fuel_oil_use = use_table_series_ceda_allocator_to_cornerstone_schema(
        load_bea_use_table(), COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS, "221200"
    )
    allocated_vec = (fuel_oil_use / fuel_oil_use.sum()) * emissions

    return (
        allocated_vec.reindex(get_allocation_sectors(), fill_value=0.0)
        * MEGATONNE_TO_KG
    )
