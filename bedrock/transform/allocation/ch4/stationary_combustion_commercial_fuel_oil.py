from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_stationary_combustion,
)
from bedrock.transform.allocation.constants import (
    COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_stationary_combustion_commercial_fuel_oil() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_stationary_combustion().loc[
        ("Commercial", "Fuel Oil")  # previously Institutional fuel oil
    ]
    fuel_oil_use = (
        load_bea_use_table()
        .loc[
            pd.Index(COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS),
            "324110",
        ]
        .astype(float)
    )
    allocated_vec = (fuel_oil_use / fuel_oil_use.sum()) * emissions

    return allocated_vec.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
