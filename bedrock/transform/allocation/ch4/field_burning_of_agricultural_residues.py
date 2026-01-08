from __future__ import annotations

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.emissions.gwp import GWP100_AR4
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.economic.units import KILOTONNE_TO_KG, MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import load_ch4_and_n2o_from_field_burning

CH4_ALLOCATION = {
    ("CH4", "Soybeans"): "1111A0",
    ("CH4", "Corn"): "1111B0",
    ("CH4", "Cotton"): "1111B0",
    ("CH4", "Lentils"): "1111B0",
    ("CH4", "Rice"): "1111B0",
    ("CH4", "Sugarcane"): "1111B0",
    ("CH4", "Wheat"): "1111B0",
}


def allocate_field_burning_of_agricultural_residues() -> pd.Series[float]:
    if get_usa_config().usa_ghg_data_year == 2022:
        ser = (
            load_ch4_and_n2o_from_field_burning() * GWP100_AR4["CH4"] * KILOTONNE_TO_KG
        )
    else:
        ser = load_ch4_and_n2o_from_field_burning() * MEGATONNE_TO_KG
    allocated = ser.groupby(CH4_ALLOCATION).sum()  # type:ignore
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0)
