from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import (
    load_bea_make_table,
    load_bea_use_table,
    use_table_series_ceda_allocator_to_cornerstone_schema,
)
from bedrock.transform.allocation.utils import get_allocation_sectors


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
    # CEDA allocator sectors aligned to Cornerstone schema when use table is Cornerstone.
    consumption_numer = use_table_series_ceda_allocator_to_cornerstone_schema(
        bea_use, get_allocation_sectors(), industrial_refrigerator
    )
    consumption_denom_ceda = float(consumption_numer.sum())
    f01000 = float(
        bea_use.loc["F01000", industrial_refrigerator]  # type: ignore[index]
        if "F01000" in bea_use.index
        else 0.0
    )
    consumption_ratio = consumption_numer / (consumption_denom_ceda + f01000)

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

    # CEDA allocator sectors aligned to Cornerstone schema when use table is Cornerstone.
    p_foam_numer = use_table_series_ceda_allocator_to_cornerstone_schema(
        bea_use, get_allocation_sectors(), p_foam
    )
    p_foam_denom_ceda = float(p_foam_numer.sum())
    p_foam_f01000 = float(
        bea_use.loc["F01000", p_foam]  # type: ignore[index]
        if "F01000" in bea_use.index
        else 0.0
    )
    p_foam_consumption_ratio = p_foam_numer / (p_foam_denom_ceda + p_foam_f01000)

    u_foam_numer = use_table_series_ceda_allocator_to_cornerstone_schema(
        bea_use, get_allocation_sectors(), u_foam
    )
    u_foam_denom_ceda = float(u_foam_numer.sum())
    u_foam_f01000 = float(
        bea_use.loc["F01000", u_foam]  # type: ignore[index]
        if "F01000" in bea_use.index
        else 0.0
    )
    u_foam_consumption_ratio = u_foam_numer / (u_foam_denom_ceda + u_foam_f01000)

    return (
        p_foam_production_ratio * p_foam_consumption_ratio
        + u_foam_production_ratio * u_foam_consumption_ratio
    )
