from __future__ import annotations

import functools

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.economic.units import COAL_MMBTU_PER_SHORT_TONNE, MEGATONNE_TO_KG
from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_mmt_co2e_across_fuel_types as _load_table_a17_mmt_co2e,
)
from bedrock.extract.allocation.epa import (
    load_tbtu_across_fuel_types as _load_table_a17_tbtu,
)
from bedrock.extract.allocation.mecs import load_mecs_3_1 as _load_mecs_3_1
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING as CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING,
)
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING as CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    NON_MECS_INDUSTRIES,
)

load_table_a17_tbtu = functools.cache(_load_table_a17_tbtu)
load_mecs_3_1 = functools.cache(_load_mecs_3_1)
load_table_a17_mmt_co2e = functools.cache(_load_table_a17_mmt_co2e)

COAL_CODE = "212100"
COAL_MECS_CODE = "Coal"


@functools.cache
def get_total_coal_emissions_to_allocate() -> float:
    return float(load_table_a17_mmt_co2e().loc["Total Coal", "Ind"])  # type: ignore


def allocate_industrial_coal() -> pd.Series[float]:
    all_mapped_industries = (
        list(CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING.keys())
        + list(CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING.keys())
        + NON_MECS_INDUSTRIES
    )
    # Ensure no duplicates in the mapping because duplicates would be
    # an error as we'd have allocated to the same industry twice
    assert len(all_mapped_industries) == len(set(all_mapped_industries))
    allocated = (
        _allocate_industrial_coal_to_industries_energy_allocation()
        + _allocate_remaining_industrial_coal_usage()
    )

    return (
        (allocated / allocated.sum())
        * get_total_coal_emissions_to_allocate()
        * MEGATONNE_TO_KG
    )


def _allocate_industrial_coal_to_industries_energy_allocation() -> pd.Series[float]:
    fraction_to_allocate = _fraction_coal_energy_to_allocate()
    mecs_3_1 = load_mecs_3_1()
    mecs_overall_coal_usage: float = mecs_3_1.loc["Total", COAL_MECS_CODE]  # type: ignore
    bea_use_table = load_bea_use_table()
    allocated_ser = pd.Series(0.0, index=CEDA_V7_SECTORS)
    for (
        ceda_industries,
        mecs_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING.items():
        total_use: float = bea_use_table.loc[list(ceda_industries), COAL_CODE].sum()
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        # The original spreadsheet just 0's out if the index canot be found
        # we replicate that logic here
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_3_1.index]
        mecs_total: float = mecs_3_1.loc[mecs_mappings_to_use, COAL_MECS_CODE].sum()
        for ceda_industry in ceda_industries:
            industry_use = bea_use_table.loc[ceda_industry, COAL_CODE]
            allocated_ser[ceda_industry] = (
                # This is L3
                get_total_coal_emissions_to_allocate()  # type: ignore
                * fraction_to_allocate  # SpecE7, EIAM86, EPAH6
                * (mecs_total / mecs_overall_coal_usage)  # EIA numerator / EIA total
                * industry_use
                / total_use
            )
    for ceda_industries, (
        mecs_mappings,
        subtract_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING.items():
        total_use: float = bea_use_table.loc[list(ceda_industries), COAL_CODE].sum()  # type: ignore
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_3_1.index]
        mecs_total: float = mecs_3_1.loc[mecs_mappings_to_use, COAL_MECS_CODE].sum()  # type: ignore
        subtract_mappings = [m for m in subtract_mappings if m in mecs_3_1.index]  # type: ignore
        subtraction_total: float = mecs_3_1.loc[
            list(subtract_mappings), COAL_MECS_CODE
        ].sum()
        allocated_total = mecs_total - subtraction_total
        for ceda_industry in ceda_industries:
            industry_use = bea_use_table.loc[ceda_industry, COAL_CODE]
            allocated_ser[ceda_industry] = (
                get_total_coal_emissions_to_allocate()  # type: ignore
                * (allocated_total / mecs_overall_coal_usage)
                * fraction_to_allocate
                * industry_use
                / total_use
            )
    return allocated_ser * MEGATONNE_TO_KG


def _allocate_remaining_industrial_coal_usage() -> pd.Series[float]:
    """
    We allocate all sectors based on MECS data and then there are a smaller
    number of sectors (NON_MECS_INDUSTRIES) that we allocate the
    remaining coal emissions to if any exist!
    """

    remaining_energy_usage: float = 1.0 - _fraction_coal_energy_to_allocate()

    allocated_ser = pd.Series(0.0, index=CEDA_V7_SECTORS)

    bea_use_table = load_bea_use_table()
    denominator: float = bea_use_table.loc[NON_MECS_INDUSTRIES, COAL_CODE].sum()
    for industry in NON_MECS_INDUSTRIES:
        use: float = bea_use_table.loc[industry, COAL_CODE]  # type: ignore
        allocated_ser[industry] = (
            get_total_coal_emissions_to_allocate()
            * remaining_energy_usage
            * use
            / denominator
        )
    return allocated_ser * MEGATONNE_TO_KG


@functools.cache
def _fraction_coal_energy_to_allocate() -> float:
    mecs_3_1 = load_mecs_3_1()
    table_a17_tbtu = load_table_a17_tbtu()

    fraction: float = (
        mecs_3_1.loc["Total", COAL_MECS_CODE]  # type: ignore
        * COAL_MMBTU_PER_SHORT_TONNE
        / table_a17_tbtu.loc["Total Coal", "Ind"]
    )

    # MECS and EPA data may be from different years, and older
    # MECS data may have more coal consumption than EPA, even though
    # MECS covers fewer sectors. If this happens, cap the fraction at 1.0
    # to avoid allocating more emissions than is available.
    return fraction if fraction <= 1 else 1.0
