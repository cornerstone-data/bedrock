from __future__ import annotations

import pandas as pd

from bedrock.transform.allocation.other_gases.common_ratios import (
    derive_make_use_ratios_for_hfcs_from_foams,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_hfc_emissions_from_ods_substitutes,
    load_hfc_pfc_emissions_from_ods_substitutes,
)


def allocate_hfc_125_foams() -> pd.Series[float]:
    table_4_99 = load_hfc_emissions_from_ods_substitutes()
    hfc_125_ratio = table_4_99.loc["HFC-125"] / table_4_99.loc["Total"]

    total_emission = (
        load_hfc_pfc_emissions_from_ods_substitutes().loc["Foams"] * hfc_125_ratio
    )

    allocated_vec = total_emission * derive_make_use_ratios_for_hfcs_from_foams()

    return allocated_vec.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
