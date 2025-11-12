from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.ceda_usa.extract.allocation.bea import load_bea_use_table
from bedrock.ceda_usa.extract.allocation.epa import (
    load_direct_n2o_from_agricultural_soils,
    load_indirect_n2o_from_agricultural_soils,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG

FERTILIZER_SECTORS: ta.List[CEDA_V7_SECTOR] = [
    "1111A0",
    "1111B0",
    "111200",
    "111300",
    "111400",
    "111900",
]

DIRECT_ACTIVITIES = [
    ("Cropland", "Synthetic Fertilizer"),
    ("Cropland", "Organic Amendment a"),
    ("Cropland", "Residue N b"),
]

INDIRECT_ACTIVITIES = [
    ("Cropland", "Volatilization & Atm. Deposition"),
    ("Cropland", "Surface Leaching & Run-Off"),
]


def allocate_fertilizer() -> pd.Series[float]:
    bea_fertilizer_use = load_bea_use_table().loc[
        pd.Index(FERTILIZER_SECTORS), "325310"
    ]
    bea_fertilizer_use = bea_fertilizer_use / bea_fertilizer_use.sum()

    total = (
        load_direct_n2o_from_agricultural_soils().loc[pd.Index(DIRECT_ACTIVITIES)].sum()
        + load_indirect_n2o_from_agricultural_soils()
        .loc[pd.Index(INDIRECT_ACTIVITIES)]
        .sum()
    )

    allocated = bea_fertilizer_use * total
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
