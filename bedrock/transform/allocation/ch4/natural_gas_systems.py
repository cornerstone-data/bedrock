from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_natural_gas_systems,
)

EPA_NATURAL_GAS_SYSTEMS_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    "Production": "211000",  # previously Field Production
    "Processing": "211000",
    "Transmission and Storage": "486000",
    "Distribution": "221200",
}


def allocate_natural_gas_systems() -> pd.Series[float]:
    ngs = load_ch4_emissions_from_natural_gas_systems()
    emissions = ngs.loc[
        pd.Index(EPA_NATURAL_GAS_SYSTEMS_TO_BEA_INDUSTRY_MAPPING.keys())
    ]
    emissions.index = pd.Index(
        [EPA_NATURAL_GAS_SYSTEMS_TO_BEA_INDUSTRY_MAPPING[i] for i in emissions.index]
    )

    allocated_vec = pd.Series(0.0, index=CEDA_V7_SECTORS)
    allocated_industries = ["211000", "221200", "486000"]
    allocated_vec[allocated_industries] = emissions.groupby(level=0).sum()

    return allocated_vec * MEGATONNE_TO_KG
