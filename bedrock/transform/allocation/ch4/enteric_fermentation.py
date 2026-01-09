from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_enteric_fermentation,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS

EPA_LIVESTOCK_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    "Beef Cattle": "1121A0",
    "Dairy Cattle": "112120",
    "Swine": "112A00",
    "Horses": "112A00",
    "Sheep": "112A00",
    "Goats": "112A00",
    "American Bison": "112A00",
    "Mules and Asses": "112A00",
}


def allocate_enteric_fermentation() -> pd.Series[float]:
    tbl = load_ch4_emissions_from_enteric_fermentation()
    emissions = tbl.loc[pd.Index(EPA_LIVESTOCK_TO_BEA_INDUSTRY_MAPPING.keys())]
    emissions.index = pd.Index(
        [EPA_LIVESTOCK_TO_BEA_INDUSTRY_MAPPING[i] for i in emissions.index]
    )

    allocated_vec = pd.Series(0.0, index=CEDA_V7_SECTORS)
    allocated_industries = ["112120", "1121A0", "112A00"]
    allocated_vec[allocated_industries] = emissions.groupby(level=0).sum()

    return allocated_vec * MEGATONNE_TO_KG
