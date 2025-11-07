from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from ceda_usa.transform.allocation.co2.industrial_petrol import (
    allocate_industrial_petrol as allocate_industrial_petrol_co2,
)
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_industrial_fuel_oil() -> pd.Series[float]:
    emissions = load_n2o_emissions_from_stationary_combustion().loc[
        ("Industrial", "Fuel Oil")
    ]
    assert isinstance(emissions, float)
    co2_allocation = allocate_industrial_petrol_co2()
    pct = co2_allocation / co2_allocation.sum()

    return emissions * pct * MEGATONNE_TO_KG
