from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.config.usa_config import get_usa_config
from bedrock.ceda_usa.utils.gwp import GWP100_AR4
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import KILOTONNE_TO_KG, MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_ch4_and_n2o_from_field_burning,
    load_recent_trends_in_ghg_emissions_and_sinks,
)

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
        emissions = (
            load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("N2Oc", "Field Burning of Agricultural Residues")
            ]
            * GWP100_AR4["N2O"]
            * KILOTONNE_TO_KG
        )
    else:
        emissions = (
            load_recent_trends_in_ghg_emissions_and_sinks().loc[
                ("N2Oc", "Field Burning of Agricultural Residues")
            ]
            * MEGATONNE_TO_KG
        )

    ser = load_ch4_and_n2o_from_field_burning()
    pct = ser.groupby(CH4_ALLOCATION).sum()  # type:ignore
    pct = pct / pct.sum()

    allocated = pct * emissions
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0)
