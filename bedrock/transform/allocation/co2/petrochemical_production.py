from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_petrochemical_production,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS

PETROCHEMICALS_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    "Carbon Black": "3252A0",
    "Ethylene": "324110",
    "Ethylene Oxide": "325411",
    "Methanol": "325411",
    "Acrylonitrile": "325411",  # TODO: why not mappped to 325190 like in ch4/petrochemical_production.py?
}


def allocate_petrochemical_production() -> pd.Series[float]:
    emissions = load_co2_emissions_from_petrochemical_production()  # in kilotonnes
    allocated = (
        emissions.groupby(PETROCHEMICALS_TO_BEA_INDUSTRY_MAPPING)  # type: ignore
        .sum()
        .reindex(CEDA_V7_SECTORS, fill_value=0)
    )
    return allocated * MEGATONNE_TO_KG
