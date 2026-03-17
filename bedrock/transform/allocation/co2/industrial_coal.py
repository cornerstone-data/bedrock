from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_mmt_co2e_across_fuel_types as _load_table_a17_mmt_co2e,
)
from bedrock.extract.allocation.epa import (
    load_tbtu_across_fuel_types as _load_table_a17_tbtu,
)
from bedrock.extract.allocation.mecs import load_mecs_3_1 as _load_mecs_3_1
from bedrock.transform.allocation.mappings.cornerstone import (
    CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    NON_MECS_INDUSTRIES,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.units import COAL_MMBTU_PER_SHORT_TONNE, MEGATONNE_TO_KG

load_table_a17_tbtu = functools.cache(_load_table_a17_tbtu)
load_mecs_3_1 = functools.cache(_load_mecs_3_1)
load_table_a17_mmt_co2e = functools.cache(_load_table_a17_mmt_co2e)

COAL_CODE = "212100"
COAL_MECS_CODE = "Coal"


def _get_mecs_3_1_naics_mappings() -> tuple[
    dict[tuple[str, ...], tuple[str, ...]],
    dict[tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]],
]:
    """Return (mapping, subtraction_mapping) for MECS 3.1 NAICS; use CORNERSTONE when schema flag is on."""
    if get_usa_config().use_cornerstone_2026_model_schema:
        return (
            CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
            CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
        )
    return (
        CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
        CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    )


@functools.cache
def get_total_coal_emissions_to_allocate() -> float:
    return float(load_table_a17_mmt_co2e().loc["Total Coal", "Ind"])  # type: ignore


def allocate_industrial_coal() -> pd.Series[float]:
    mapping, subtraction_mapping = _get_mecs_3_1_naics_mappings()
    all_mapped_industries = (
        list(mapping.keys()) + list(subtraction_mapping.keys()) + NON_MECS_INDUSTRIES
    )
    # Ensure no duplicates in the mapping because duplicates would be
    # an error as we'd have allocated to the same industry twice
    assert len(all_mapped_industries) == len(set(all_mapped_industries))
    target_sectors = get_allocation_sectors()
    part1 = _allocate_industrial_coal_to_industries_energy_allocation()
    part2 = _allocate_remaining_industrial_coal_usage()
    allocated = part1.reindex(target_sectors, fill_value=0.0) + part2.reindex(
        target_sectors, fill_value=0.0
    )

    total_allocated = allocated.sum()
    if total_allocated == 0 or pd.isna(total_allocated):
        return allocated * MEGATONNE_TO_KG
    return (
        (allocated / total_allocated)
        * get_total_coal_emissions_to_allocate()
        * MEGATONNE_TO_KG
    )


def _allocate_industrial_coal_to_industries_energy_allocation() -> pd.Series[float]:
    mapping, subtraction_mapping = _get_mecs_3_1_naics_mappings()
    fraction_to_allocate = _fraction_coal_energy_to_allocate()
    mecs_3_1 = load_mecs_3_1()
    mecs_overall_coal_usage: float = mecs_3_1.loc["Total", COAL_MECS_CODE]
    bea_use_table = load_bea_use_table()
    use_series = bea_use_table.loc[:, COAL_CODE]
    use_cornerstone = get_usa_config().use_cornerstone_2026_model_schema
    allocated_ser = pd.Series(0.0, index=get_allocation_sectors())
    for (
        ceda_industries,
        mecs_mappings,
    ) in mapping.items():
        inds = list(ceda_industries)
        total_use_ser = (
            use_series.reindex(inds, fill_value=1.0)
            if use_cornerstone
            else use_series.loc[inds]
        )
        total_use: float = float(total_use_ser.sum())
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        # The original spreadsheet just 0's out if the index cannot be found
        # we replicate that logic here
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_3_1.index]
        mecs_total: float = float(
            mecs_3_1.loc[mecs_mappings_to_use, COAL_MECS_CODE].fillna(0).sum()
        )
        for ceda_industry in ceda_industries:
            industry_use = float(total_use_ser[ceda_industry])
            val = (
                get_total_coal_emissions_to_allocate()
                * fraction_to_allocate  # SpecE7, EIAM86, EPAH6
                * (mecs_total / mecs_overall_coal_usage)  # EIA numerator / EIA total
                * industry_use
                / total_use
            )
            allocated_ser[ceda_industry] = 0.0 if pd.isna(val) else float(val)
    for ceda_industries, (
        mecs_mappings,
        subtract_mappings,
    ) in subtraction_mapping.items():
        inds_sub = list(ceda_industries)
        total_use_ser_sub = (
            use_series.reindex(inds_sub, fill_value=1.0)
            if use_cornerstone
            else use_series.loc[inds_sub]
        )
        total_use_sub: float = float(total_use_ser_sub.sum())
        if total_use_sub == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_3_1.index]
        mecs_total = float(
            mecs_3_1.loc[mecs_mappings_to_use, COAL_MECS_CODE].fillna(0).sum()
        )
        subtract_mappings = [m for m in subtract_mappings if m in mecs_3_1.index]
        subtraction_total: float = float(
            mecs_3_1.loc[list(subtract_mappings), COAL_MECS_CODE].fillna(0).sum()
        )
        allocated_total = mecs_total - subtraction_total
        for ceda_industry in ceda_industries:
            industry_use = float(total_use_ser_sub[ceda_industry])
            val = (
                get_total_coal_emissions_to_allocate()
                * (allocated_total / mecs_overall_coal_usage)
                * fraction_to_allocate
                * industry_use
                / total_use_sub
            )
            allocated_ser[ceda_industry] = 0.0 if pd.isna(val) else float(val)
    return allocated_ser * MEGATONNE_TO_KG


def _allocate_remaining_industrial_coal_usage() -> pd.Series[float]:
    """
    We allocate all sectors based on MECS data and then there are a smaller
    number of sectors (NON_MECS_INDUSTRIES) that we allocate the
    remaining coal emissions to if any exist!
    """

    remaining_energy_usage: float = 1.0 - _fraction_coal_energy_to_allocate()

    allocated_ser = pd.Series(0.0, index=get_allocation_sectors())

    bea_use_table = load_bea_use_table()
    use_series = bea_use_table.loc[:, COAL_CODE]
    denominator: float = float(use_series.reindex(NON_MECS_INDUSTRIES, fill_value=0.0).sum())
    for industry in NON_MECS_INDUSTRIES:
        use = float(use_series.reindex([industry], fill_value=0.0).iloc[0])
        val = (
            get_total_coal_emissions_to_allocate()
            * remaining_energy_usage
            * use
            / denominator
        )
        if industry in allocated_ser.index:
            allocated_ser[industry] = 0.0 if (pd.isna(val) or denominator == 0) else float(val)
    return allocated_ser * MEGATONNE_TO_KG


@functools.cache
def _fraction_coal_energy_to_allocate() -> float:
    mecs_3_1 = load_mecs_3_1()
    table_a17_tbtu = load_table_a17_tbtu()

    fraction: float = (
        mecs_3_1.loc["Total", COAL_MECS_CODE]
        * COAL_MMBTU_PER_SHORT_TONNE
        / table_a17_tbtu.loc["Total Coal", "Ind"]
    )

    # MECS and EPA data may be from different years, and older
    # MECS data may have more coal consumption than EPA, even though
    # MECS covers fewer sectors. If this happens, cap the fraction at 1.0
    # to avoid allocating more emissions than is available.
    return fraction if fraction <= 1 else 1.0


# pct = _fraction_coal_energy_to_allocate() # 1.0 (420 MMBTU / 380.9)
