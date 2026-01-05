from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_fossil_fuels_for_non_energy_uses,
)


def allocate_non_energy_fuels_coal_coke() -> pd.Series[float]:
    emissions = (
        load_co2_emissions_from_fossil_fuels_for_non_energy_uses()
        .loc[
            [
                ("Industry", "Industrial Coking Coal"),
                ("Industry", "Industrial Other Coal"),
            ]
        ]
        .sum()
    )
    allocated = pd.Series(
        {"2122A0": emissions}
    )  # Iron, gold, silver, and other metal ore mining
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
