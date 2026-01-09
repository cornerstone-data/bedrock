from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.epa import (
    load_direct_n2o_from_agricultural_soils,
)
from bedrock.extract.allocation.usda import load_crop_land_area_harvested

ACTIVITIES = [
    ("Cropland", "Mineralization and Asymbiotic Fixation"),
    ("Cropland", "Drained Organic Soils"),
]


def allocate_mineralization() -> pd.Series[float]:
    table_5_18 = load_direct_n2o_from_agricultural_soils()
    total = table_5_18.loc[pd.Index(ACTIVITIES)].sum()

    harvested = load_crop_land_area_harvested()
    pct = harvested / harvested.sum()

    return (pct * total).reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
