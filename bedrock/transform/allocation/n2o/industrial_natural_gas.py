from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from bedrock.transform.allocation.co2.industrial_natural_gas import (
    allocate_industrial_natural_gas as allocate_industrial_natural_gas_co2,
)


def allocate_industrial_natural_gas() -> pd.Series[float]:
    emissions = load_n2o_emissions_from_stationary_combustion().loc[
        ("Industrial", "Natural Gas")
    ]
    assert isinstance(emissions, float)
    co2_allocation = allocate_industrial_natural_gas_co2()
    pct = co2_allocation / co2_allocation.sum()

    return emissions * pct * MEGATONNE_TO_KG
