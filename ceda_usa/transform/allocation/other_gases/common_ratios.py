from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.bea import load_bea_make_table, load_bea_use_table
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def derive_make_use_ratios_for_hfcs_from_other_sources() -> pd.Series[float]:
    industrial_refrigerator = "333415"
    bea_make = load_bea_make_table()
    industrial_refrigerator_production = bea_make.loc[
        industrial_refrigerator, industrial_refrigerator
    ]
    household_refrigerator_production = bea_make.loc["335222", "335222"]
    production_ratio = industrial_refrigerator_production / (  # type: ignore
        industrial_refrigerator_production + household_refrigerator_production  # type: ignore
    )

    bea_use = load_bea_use_table()
    consumption_ratio = bea_use.loc[
        pd.Index(CEDA_V7_SECTORS), industrial_refrigerator
    ] / (
        bea_use.loc[
            pd.Index(CEDA_V7_SECTORS + ["F01000"]), industrial_refrigerator
        ].sum()
    )

    return production_ratio * consumption_ratio


def derive_make_use_ratios_for_hfcs_from_foams() -> pd.Series[float]:
    p_foam = "326140"  # Polystyrene foam
    u_foam = "326150"  # Urethane and other foam
    p_foam_idx = pd.Index([p_foam])
    u_foam_idx = pd.Index([u_foam])
    bea_make = load_bea_make_table()
    p_foam_production = bea_make.loc[p_foam_idx.union(u_foam_idx), p_foam].sum()
    u_foam_production = bea_make.loc[p_foam_idx.union(u_foam_idx), u_foam].sum()
    total_foam_production = p_foam_production + u_foam_production
    p_foam_production_ratio = p_foam_production / total_foam_production
    u_foam_production_ratio = u_foam_production / total_foam_production

    bea_use = load_bea_use_table()
    p_foam_consumption_ratio = bea_use.loc[pd.Index(CEDA_V7_SECTORS), p_foam] / (
        bea_use.loc[pd.Index(CEDA_V7_SECTORS + ["F01000"]), p_foam].sum()
    )
    u_foam_consumption_ratio = bea_use.loc[pd.Index(CEDA_V7_SECTORS), u_foam] / (
        bea_use.loc[pd.Index(CEDA_V7_SECTORS + ["F01000"]), u_foam].sum()
    )

    return (
        p_foam_production_ratio * p_foam_consumption_ratio
        + u_foam_production_ratio * u_foam_consumption_ratio
    )
