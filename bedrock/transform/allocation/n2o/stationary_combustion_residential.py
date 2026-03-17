from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_n2o_emissions_from_stationary_combustion,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_stationary_combustion_residential() -> pd.Series[float]:
    total = load_n2o_emissions_from_stationary_combustion().loc[
        ("Residential", "TOTAL")
    ]

    return (pd.Series({"F01000": total})).reindex(
        get_allocation_sectors(), fill_value=0.0
    ) * MEGATONNE_TO_KG
