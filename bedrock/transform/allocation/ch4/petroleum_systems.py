from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_petroleum_systems,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS

EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    # NOTE: No "Production Voluntary Reductions" found in Table 3-36 or 3-37
    # Production Field Operations (Net) therefore assumed to be same as total
    "Total ": "211000",
}


def allocate_petroleum_systems() -> pd.Series[float]:
    ps = load_ch4_emissions_from_petroleum_systems()
    emissions = ps.loc[pd.Index(EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING.keys())]
    emissions.index = pd.Index(
        [EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING[i] for i in emissions.index]
    )

    allocated_vec = emissions.reindex(CEDA_V7_SECTORS, fill_value=0.0)

    return allocated_vec * MEGATONNE_TO_KG
