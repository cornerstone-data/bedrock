from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_direct_n2o_from_agricultural_soils,
)
from bedrock.extract.allocation.usda import load_crop_land_area_harvested
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG

ACTIVITIES = [
    ("Cropland", "Mineralization and Asymbiotic Fixation"),
    ("Cropland", "Drained Organic Soils"),
]


def allocate_mineralization() -> pd.Series[float]:
    table_5_18 = load_direct_n2o_from_agricultural_soils()
    total = table_5_18.loc[pd.Index(ACTIVITIES)].sum()

    harvested = load_crop_land_area_harvested()
    pct = harvested / harvested.sum()

    return (pct * total).reindex(
        get_allocation_sectors(), fill_value=0.0
    ) * MEGATONNE_TO_KG
