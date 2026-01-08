from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from bedrock.transform.allocation.co2.industrial_coal import (
    allocate_industrial_coal as allocate_industrial_coal_co2,
)


def allocate_industrial_coal() -> pd.Series[float]:
    emissions = load_n2o_emissions_from_stationary_combustion().loc[
        ("Industrial", "Coal")
    ]
    assert isinstance(emissions, float)
    co2_allocation = allocate_industrial_coal_co2()
    pct = co2_allocation / co2_allocation.sum()

    return emissions * pct * MEGATONNE_TO_KG
