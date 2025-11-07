from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.epa import (
    load_pfc_hfc_sf6_nf3_n2o_emissions_from_semiconductor_manufacture,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_pfc_c3f8_semiconductor_manufacture() -> pd.Series[float]:
    table_4_94 = load_pfc_hfc_sf6_nf3_n2o_emissions_from_semiconductor_manufacture()
    emissions = pd.Series({"334413": table_4_94.loc["C3F8 "]})
    # only allocating to 334413 - semiconductor manufacture
    return emissions.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
