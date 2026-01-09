from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_soda_ash_prodution,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_soda_ash_production_and_consumption() -> pd.Series[float]:
    sap = load_co2_emissions_from_soda_ash_prodution()
    emissions = sap.loc["Soda Ash Production"]
    allocated = pd.Series({"325190": emissions})
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
