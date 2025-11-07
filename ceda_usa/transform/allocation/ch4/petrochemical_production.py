from __future__ import annotations

import typing as ta

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_ch4_emissions_from_petrochemical_production,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG

PETROCHEMICALS_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    "Acrylonitrile": "325190",
}


def allocate_petrochemical_production() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_petrochemical_production()

    allocated_vec = (
        emissions.groupby(PETROCHEMICALS_TO_BEA_INDUSTRY_MAPPING)  # type: ignore
        .sum()
        .reindex(CEDA_V7_SECTORS, fill_value=0)
    )

    return allocated_vec * MEGATONNE_TO_KG
