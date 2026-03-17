from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_make_table
from bedrock.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG

MAGNESIUM_PRODUCTION_INDUSTRY_CODES = ["331410", "331490", "331520"]


def allocate_sf6_magnesium_production() -> pd.Series[float]:
    make = (
        load_bea_make_table()
        .sum(axis=1)
        .loc[pd.Index(MAGNESIUM_PRODUCTION_INDUSTRY_CODES)]
    )
    emissions = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("SF6", "Magnesium Production and Processing")
    ] * (make / make.sum())

    return emissions.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
