from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_hfc_emissions_from_ods_substitutes,
    load_hfc_pfc_emissions_from_ods_substitutes,
)
from ceda_usa.transform.allocation.other_gases.common_ratios import (
    derive_make_use_ratios_for_hfcs_from_foams,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_hfc_134a_foams() -> pd.Series[float]:
    table_4_99 = load_hfc_emissions_from_ods_substitutes()
    hfc_134a_ratio = table_4_99.loc["HFC-134a"] / table_4_99.loc["Total"]

    total_emission = (
        load_hfc_pfc_emissions_from_ods_substitutes().loc["Foams"] * hfc_134a_ratio
    )

    allocated_vec = total_emission * derive_make_use_ratios_for_hfcs_from_foams()

    return allocated_vec.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
