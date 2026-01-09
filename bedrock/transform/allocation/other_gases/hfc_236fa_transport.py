from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_hfc_emissions_from_ods_substitutes,
    load_hfc_emissions_from_transportation_sources,
)
from bedrock.transform.allocation.other_gases.constants import (
    TRANSPORTATION_SOURCE_TO_BEA_INDUSTRY_MAPPING,
)


def allocate_hfc_236fa_transport() -> pd.Series[float]:
    table_a118 = load_hfc_emissions_from_transportation_sources()
    table_4_99 = load_hfc_emissions_from_ods_substitutes()
    hfc_236fa_ratio = table_4_99.loc["HFC-236fa"] / table_4_99.loc["Total"]
    allocated_vec = (
        table_a118.loc[
            pd.Index(
                TRANSPORTATION_SOURCE_TO_BEA_INDUSTRY_MAPPING.keys(),
            )
        ]
        .groupby(TRANSPORTATION_SOURCE_TO_BEA_INDUSTRY_MAPPING)  # type: ignore
        .sum()
    ) * hfc_236fa_ratio

    return allocated_vec.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
