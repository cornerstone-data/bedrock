from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_ch4_and_n2o_emissions_from_manure_management,
)

EPA_LIVESTOCK_TO_BEA_INDUSTRY_ALLOCATION: ta.Dict[
    ta.Tuple[str, str], CEDA_V7_SECTOR
] = {
    ("CH4a", "Dairy Cattle"): "112120",
    ("CH4a", "Beef Cattle"): "1121A0",
    ("CH4a", "Swine"): "112A00",
    ("CH4a", "Sheep"): "112A00",
    ("CH4a", "Goats"): "112A00",
    ("CH4a", "Horses"): "112A00",
    ("CH4a", "American Bison"): "112A00",
    ("CH4a", "Mules and Asses"): "112A00",
    ("CH4a", "Poultry"): "112300",
}


def allocate_manure_management() -> pd.Series[float]:
    ser = load_ch4_and_n2o_emissions_from_manure_management()
    emissions = ser.loc[pd.Index(EPA_LIVESTOCK_TO_BEA_INDUSTRY_ALLOCATION.keys())]

    allocated_vec = (
        emissions.groupby(EPA_LIVESTOCK_TO_BEA_INDUSTRY_ALLOCATION)  # type: ignore
        .sum()
        .reindex(CEDA_V7_SECTORS, fill_value=0)
    )

    return allocated_vec * MEGATONNE_TO_KG
