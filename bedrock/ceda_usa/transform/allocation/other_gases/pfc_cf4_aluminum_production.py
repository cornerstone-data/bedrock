from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.extract.allocation.epa import (
    load_pfc_emissions_from_aluminum_production,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_pfc_cf4_aluminum_production() -> pd.Series[float]:
    table_4_80 = load_pfc_emissions_from_aluminum_production()
    emissions = pd.Series({"331313": table_4_80.loc["CF4"]})
    # only allocating to 331313 - primary aluminum production

    return emissions.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
