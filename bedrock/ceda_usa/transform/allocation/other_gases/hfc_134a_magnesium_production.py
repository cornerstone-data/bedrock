from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.extract.allocation.bea import load_bea_make_table
from bedrock.ceda_usa.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG

EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING = {
    "331410": ["331313", "331410", "331490"],
    "331490": ["331410", "331490", "331520"],
    "331520": ["331490", "331520", "33211A"],
}


def allocate_hfc_134a_magnesium_production() -> pd.Series[float]:
    industry_output = load_bea_make_table().sum(axis=1)
    make_ratio = pd.Series(
        [
            industry_output.loc[pd.Index([key])][0]
            / industry_output[
                pd.Index(EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING[key])
            ].sum()
            for key in EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING.keys()
        ],
        index=EPA_EMISSION_SOURCE_TO_BEA_INDUSTRY_MAPPING.keys(),
    )

    emissions = (
        load_recent_trends_in_ghg_emissions_and_sinks().loc[
            ("HFCs", "Magnesium Production and Processing")
        ]
        * make_ratio
    )

    return emissions.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
