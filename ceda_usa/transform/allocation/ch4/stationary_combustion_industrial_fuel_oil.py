from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_ch4_emissions_from_stationary_combustion,
)
from ceda_usa.transform.allocation.co2 import (
    allocate_industrial_petrol as allocate_industrial_petrol_co2,
)
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_industrial_fuel_oil() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_stationary_combustion().loc[
        ("Industrial", "Fuel Oil")
    ]
    co2_emissions = allocate_industrial_petrol_co2()
    co2_ratio = co2_emissions / co2_emissions.sum()
    return emissions * co2_ratio * MEGATONNE_TO_KG
