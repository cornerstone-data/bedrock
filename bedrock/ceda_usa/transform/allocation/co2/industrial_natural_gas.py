from __future__ import annotations

import functools

import pandas as pd

from bedrock.ceda_usa.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING as CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING,
)
from bedrock.ceda_usa.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING as CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.ceda_usa.transform.allocation.mappings.v7.ceda_mecs import (
    NON_MECS_INDUSTRIES,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG, NAT_GAS_BCF_TO_TRILLION_BTU
from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_mmt_co2e_across_fuel_types as _load_table_a17_mmt_co2e,
)
from bedrock.extract.allocation.epa import (
    load_tbtu_across_fuel_types as _load_table_a17_tbtu,
)
from bedrock.extract.allocation.mecs import load_mecs_3_1 as _load_mecs_3_1

load_table_a17_tbtu = functools.cache(_load_table_a17_tbtu)
load_mecs_3_1 = functools.cache(_load_mecs_3_1)
load_table_a17_mmt_co2e = functools.cache(_load_table_a17_mmt_co2e)

NAT_GAS_CODE = "221200"
NAT_GAS_MECS_CODE = "Natural Gas(d)"


@functools.cache
def get_total_natural_gas_emissions_to_allocate() -> float:
    return float(load_table_a17_mmt_co2e().loc["Natural Gas", "Ind"])  # type: ignore


# This code ignores the use table entirely, but also happens to have 0
# Essentially, MECS and BEA use table don't match up
SPECIAL_EXCEPTION_CODE = "316000"


def allocate_industrial_natural_gas() -> pd.Series[float]:
    all_mapped_industries = (
        list(CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING.keys())
        + list(CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING.keys())
        + NON_MECS_INDUSTRIES
    )
    # Ensure no duplicates in the mapping because duplicates would be
    # an error as we'd have allocated to the same industry twice
    assert len(all_mapped_industries) == len(set(all_mapped_industries))
    allocated = (
        _allocate_industrial_nat_gas_to_industries_energy_allocation()
        + _allocate_remaining_industrial_nat_gas_usage()
    )

    return (
        allocated
        / allocated.sum()
        * get_total_natural_gas_emissions_to_allocate()
        * MEGATONNE_TO_KG
    )


def _allocate_industrial_nat_gas_to_industries_energy_allocation() -> pd.Series[float]:
    fraction_to_allocate = _fraction_natural_gas_energy_to_allocate()
    mecs_3_1 = load_mecs_3_1()
    mecs_overall_nat_gas_usage: float = mecs_3_1.loc["Total", NAT_GAS_MECS_CODE]  # type: ignore
    bea_use_table = load_bea_use_table()

    allocated_ser = pd.Series(0.0, index=CEDA_V7_SECTORS)
    for (
        ceda_industries,
        mecs_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_NAICS_MAPPING.items():
        total_use: float = bea_use_table.loc[list(ceda_industries), NAT_GAS_CODE].sum()
        if total_use == 0 and SPECIAL_EXCEPTION_CODE not in ceda_industries:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_mappings = [m for m in mecs_mappings if m in mecs_3_1.index]  # type: ignore
        mecs_total: float = mecs_3_1.loc[list(mecs_mappings), NAT_GAS_MECS_CODE].sum()
        for ceda_industry in ceda_industries:
            industry_use = bea_use_table.loc[ceda_industry, NAT_GAS_CODE]
            if ceda_industry == SPECIAL_EXCEPTION_CODE:
                total_use = 1
                industry_use = 1
            allocated_ser[ceda_industry] = (
                # This is L3
                get_total_natural_gas_emissions_to_allocate()  # type: ignore
                * fraction_to_allocate  # SpecE7, EIAM86, EPAH6
                * (mecs_total / mecs_overall_nat_gas_usage)  # EIA numerator / EIA total
                * industry_use
                / total_use
            )

    for ceda_industries, (
        mecs_mappings,
        subtract_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_NAICS_SUBTRACTION_MAPPING.items():
        total_use: float = bea_use_table.loc[list(ceda_industries), NAT_GAS_CODE].sum()  # type: ignore
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_3_1.index]
        mecs_total: float = mecs_3_1.loc[mecs_mappings_to_use, NAT_GAS_MECS_CODE].sum()  # type: ignore
        subtract_mappings = [m for m in subtract_mappings if m in mecs_3_1.index]  # type: ignore
        subtraction_total: float = mecs_3_1.loc[
            list(subtract_mappings), NAT_GAS_MECS_CODE
        ].sum()
        allocated_total = mecs_total - subtraction_total
        for ceda_industry in ceda_industries:
            industry_use = bea_use_table.loc[ceda_industry, NAT_GAS_CODE]
            allocated_ser[ceda_industry] = (
                get_total_natural_gas_emissions_to_allocate()  # type: ignore
                * (allocated_total / mecs_overall_nat_gas_usage)
                * fraction_to_allocate
                * industry_use
                / total_use
            )
    return allocated_ser * MEGATONNE_TO_KG


def _allocate_remaining_industrial_nat_gas_usage() -> pd.Series[float]:
    """
    We allocate all sectors based on MECS data and then there are a smaller
    number of sectors (NON_MECS_INDUSTRIES) that we allocate the remaining natural gas emissions
    """
    NAT_GAS_INDUSTRIES = NON_MECS_INDUSTRIES + [NAT_GAS_CODE]
    VERY_SPECIAL_NAT_GAS_CODES_WITH_DIFF_FORUMLA = ["1111A0", "1111B0"]

    remaining_energy_usage: float = 1.0 - _fraction_natural_gas_energy_to_allocate()

    allocated_ser = pd.Series(0.0, index=CEDA_V7_SECTORS)
    if remaining_energy_usage < 0:
        return allocated_ser

    bea_use_table = load_bea_use_table()
    denominator: float = bea_use_table.loc[NAT_GAS_INDUSTRIES, NAT_GAS_CODE].sum()
    for industry in NAT_GAS_INDUSTRIES:
        if industry in VERY_SPECIAL_NAT_GAS_CODES_WITH_DIFF_FORUMLA:
            denom_to_use: float = bea_use_table.loc[
                NON_MECS_INDUSTRIES, NAT_GAS_CODE
            ].sum()
        else:
            denom_to_use = denominator
        use: float = bea_use_table.loc[industry, NAT_GAS_CODE]  # type: ignore
        allocated_ser[industry] = (
            get_total_natural_gas_emissions_to_allocate()
            * remaining_energy_usage
            * use
            / denom_to_use
        )
    return allocated_ser * MEGATONNE_TO_KG


@functools.cache
def _fraction_natural_gas_energy_to_allocate() -> float:
    mecs_3_1 = load_mecs_3_1()
    table_a17_tbtu = load_table_a17_tbtu()
    return (
        mecs_3_1.loc["Total", NAT_GAS_MECS_CODE]  # type: ignore
        * NAT_GAS_BCF_TO_TRILLION_BTU  # type: ignore
        / table_a17_tbtu.loc["Natural Gas", "Ind"]
    )
