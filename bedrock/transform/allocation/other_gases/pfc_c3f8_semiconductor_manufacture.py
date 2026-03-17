from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_pfc_hfc_sf6_nf3_n2o_emissions_from_semiconductor_manufacture,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_pfc_c3f8_semiconductor_manufacture() -> pd.Series[float]:
    table_4_94 = load_pfc_hfc_sf6_nf3_n2o_emissions_from_semiconductor_manufacture()
    emissions = pd.Series({"334413": table_4_94.loc["C3F8 "]})
    # only allocating to 334413 - semiconductor manufacture
    return emissions.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
