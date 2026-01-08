from __future__ import annotations

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_direct_n2o_from_agricultural_soils,
    load_indirect_n2o_from_agricultural_soils,
)
from bedrock.extract.allocation.usda import load_animal_operation_land


def allocate_soil_management_grassland() -> pd.Series[float]:
    tbl_5_18 = load_direct_n2o_from_agricultural_soils()
    tbl_5_19 = load_indirect_n2o_from_agricultural_soils()

    total = tbl_5_18.loc[("Grassland", "TOTAL")] + tbl_5_19.loc[("Grassland", "TOTAL")]
    assert isinstance(total, float)

    land = load_animal_operation_land()
    pct = land / land.sum()

    return (pct * total).reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
