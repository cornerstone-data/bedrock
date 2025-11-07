from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_residential() -> pd.Series[float]:
    total = load_n2o_emissions_from_stationary_combustion().loc[
        ("Residential", "TOTAL")
    ]

    return (pd.Series({"F01000": total})).reindex(
        CEDA_V7_SECTORS, fill_value=0.0
    ) * MEGATONNE_TO_KG
