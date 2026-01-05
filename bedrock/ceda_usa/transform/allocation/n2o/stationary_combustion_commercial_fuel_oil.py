from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from bedrock.ceda_usa.transform.allocation.constants import (
    COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_commercial_fuel_oil() -> pd.Series[float]:
    total = load_n2o_emissions_from_stationary_combustion().loc[
        ("Commercial", "Fuel Oil")
    ]

    fuel_oil_use = (
        load_bea_use_table()
        .loc[
            pd.Index(COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS),
            "324110",
        ]
        .astype(float)
    )

    allocated_vec = (fuel_oil_use / fuel_oil_use.sum()) * total

    return allocated_vec.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
