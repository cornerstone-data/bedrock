from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_fossil_fuels_for_non_energy_uses,
)
from bedrock.extract.allocation.mecs import load_mecs_2_1
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

logger = logging.getLogger(__name__)


def allocate_non_energy_fuels_petrol() -> pd.Series[float]:
    petrol_products = [
        "Asphalt & Road Oil",
        "HGL b",  # was referred to as "LPG" in 2018 GHG Inventory
        "Lubricants",
        "Natural Gasoline c",  # was referred to as "Pentanes Plus" in 2018 GHG Inventory
        "Naphtha (<401° F)",
        "Other Oil (>401° F)",
        "Still Gas",
        "Petroleum Coke",
        "Special Naphtha",
        "Distillate Fuel Oil",
        "Waxes",
        "Miscellaneous Products",
    ]
    allocated = pd.Series(0.0, index=CEDA_V7_SECTORS)

    # Emissions fron non-energy use of petrol products are categorized to 3 major buckets:
    # 1. Asphalt & Road Oil
    # 2. HGL
    # 3. Remaining petrol products

    # We want to allocate emissions from use of asphalt to ashpalt industries only, according to their use of "Other(e)" in MECS 2.1
    # For HGL emissions, we want to allocate them to all industries that use HGL according to HGL (excluding natural gasoline)(d) in MECS 2.1
    # For the remaining emissions, we want to allocate them to all industries except asphalt industries according to Other(e) in MECS 2.1
    logger.info("NOT reverting to V5 allocation changes.")
    emissions_total = (
        load_co2_emissions_from_fossil_fuels_for_non_energy_uses()
        .loc[pd.MultiIndex.from_product([["Industry"], petrol_products])]
        .sum()
    )
    emissions_asphalt = (
        load_co2_emissions_from_fossil_fuels_for_non_energy_uses()
        .loc[pd.MultiIndex.from_product([["Industry"], ["Asphalt & Road Oil"]])]
        .squeeze()
    )
    emissions_hgl = (
        load_co2_emissions_from_fossil_fuels_for_non_energy_uses()
        .loc[pd.MultiIndex.from_product([["Industry"], ["HGL b"]])]
        .squeeze()
    )
    emissions_remaining = emissions_total - emissions_asphalt - emissions_hgl  # type: ignore

    mecs_2_1_hgl = load_mecs_2_1()["HGL (excluding natural gasoline)(d)"]
    mecs_2_1_hgl_sum = mecs_2_1_hgl["Total"]
    mecs_2_1_other = load_mecs_2_1()["Other(e)"]
    mecs_2_1_other_sum = mecs_2_1_other["Total"]
    mecs_2_1_other_sum_wo_asphalt = (
        mecs_2_1_other_sum - mecs_2_1_other[["324121", "324122"]].sum()
    )

    use = load_bea_use_table()["324110"].astype(float)
    for (
        ceda_industries,
        mecs_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING.items():
        total_use: float = use.loc[list(ceda_industries)].sum()
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_hgl_subtotal: float = mecs_2_1_hgl[
            [m for m in mecs_mappings if m in mecs_2_1_hgl.index]
        ].sum()
        mecs_other_subtotal: float = mecs_2_1_other[
            [m for m in mecs_mappings if m in mecs_2_1_other.index]
        ].sum()

        for ceda_industry in ceda_industries:
            if len(ceda_industries) == 1:
                assert (
                    use[ceda_industry] == total_use
                ), f"There is only one sector in ceda_industries {ceda_industries}, but use by the industry {use[ceda_industry]} != total_use ({total_use})"
            else:
                assert (
                    use[ceda_industry] <= total_use
                ), f"There are more than one sector in ceda_industries {ceda_industries}, but use by a child industry {ceda_industry} ({use[ceda_industry]}) > total_use ({total_use})"
            if ceda_industry in ("324121", "324122"):
                # Allocate asphalt and HGL emissions to asphalt industries (324121 and 324122)
                allocated[ceda_industry] = (
                    emissions_asphalt
                    * (
                        mecs_2_1_other[ceda_industry]
                        / mecs_2_1_other[["324121", "324122"]].sum()
                    )
                    * use[ceda_industry]
                    / total_use
                ) + (
                    emissions_hgl
                    * (mecs_2_1_hgl[ceda_industry] / mecs_2_1_hgl_sum)
                    * use[ceda_industry]
                    / total_use
                )
            else:
                allocated[ceda_industry] = (
                    emissions_remaining
                    * (mecs_other_subtotal / mecs_2_1_other_sum_wo_asphalt)
                    * use[ceda_industry]
                    / total_use
                ) + (
                    emissions_hgl
                    * (mecs_hgl_subtotal / mecs_2_1_hgl_sum)
                    * use[ceda_industry]
                    / total_use
                )
    for ceda_industries, (
        mecs_mappings,
        subtract_mappings,
    ) in CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING.items():
        total_use = use.loc[list(ceda_industries)].sum()
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_mappings_to_use = [m for m in mecs_mappings if m in mecs_2_1_other.index]
        mecs_other_total: float = mecs_2_1_other.loc[mecs_mappings_to_use].sum()
        mecs_other_subtract_mappings = [
            m for m in subtract_mappings if m in mecs_2_1_other.index
        ]
        mecs_other_subtraction_total: float = mecs_2_1_other.loc[
            list(mecs_other_subtract_mappings),
        ].sum()
        mecs_other_allocated_total = mecs_other_total - mecs_other_subtraction_total

        mecs_hgl_total: float = mecs_2_1_hgl.loc[mecs_mappings_to_use].sum()
        mecs_hgl_subtract_mappings = [
            m for m in subtract_mappings if m in mecs_2_1_hgl.index
        ]
        mecs_hgl_subtraction_total: float = mecs_2_1_hgl.loc[
            list(mecs_hgl_subtract_mappings),
        ].sum()
        mecs_hgl_allocated_total = mecs_hgl_total - mecs_hgl_subtraction_total

        for ceda_industry in ceda_industries:
            industry_use = use.loc[ceda_industry]
            allocated[ceda_industry] = (
                emissions_remaining
                * (mecs_other_allocated_total / mecs_2_1_other_sum)
                * industry_use
                / total_use
            ) + (
                emissions_hgl
                * (mecs_hgl_allocated_total / mecs_2_1_hgl_sum)
                * industry_use
                / total_use
            )
    # There might be small under/over allocation due to independent rounding in MECS 2.1 table
    # Force the sum to be equal to emissions if 5% difference, otherwise raise an error
    if np.isclose(allocated.sum(), emissions_total, rtol=5e-2):
        allocated = emissions_total * allocated / allocated.sum()
    else:
        raise ValueError(
            f"Allocated emissions {allocated.sum()} MMT do not match total emissions {emissions_total} MMT."
        )

    return allocated * MEGATONNE_TO_KG
