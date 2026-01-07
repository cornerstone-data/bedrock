from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.transform.allocation.co2 import (
    allocate_industrial_coal as allocate_industrial_coal_co2,
)
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_stationary_combustion,
)


def allocate_stationary_combustion_industrial_coal() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_stationary_combustion().loc[
        ("Industrial", "Coal")
    ]
    co2_emissions = allocate_industrial_coal_co2()
    co2_ratio = co2_emissions / co2_emissions.sum()
    return emissions * co2_ratio * MEGATONNE_TO_KG
