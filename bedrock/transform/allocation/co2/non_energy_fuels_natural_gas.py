from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_fossil_fuels_for_non_energy_uses,
)
from bedrock.extract.allocation.mecs import load_mecs_2_1
from bedrock.transform.allocation.mappings.cornerstone import (
    CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.transform.allocation.mappings.v7.ceda_mecs import (
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
    CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
)
from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.units import MEGATONNE_TO_KG

logger = logging.getLogger(__name__)


def _get_mecs_2_1_naics_mappings() -> (
    tuple[
        dict[tuple[str, ...], tuple[str, ...]],
        dict[tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]],
    ]
):
    """Return (mapping, subtraction_mapping) for MECS 2.1 NAICS; use CORNERSTONE when schema flag is on."""
    if get_usa_config().use_cornerstone_2026_model_schema:
        return (
            CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
            CORNERSTONE_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
        )
    return (
        CEDA_INDUSTRY_TO_MECS_2_1_NAICS_MAPPING,
        CEDA_INDUSTRY_TO_MECS_2_1_NAICS_SUBTRACTION_MAPPING,
    )


def allocate_non_energy_fuels_natural_gas() -> pd.Series[float]:
    mapping, subtraction_mapping = _get_mecs_2_1_naics_mappings()
    emissions = (
        load_co2_emissions_from_fossil_fuels_for_non_energy_uses()
        .loc[("Industry", "Natural Gas to Chemical Plants")]  # type: ignore
        .squeeze()
    )
    use = load_bea_use_table().loc[:, "221200"].astype(float)
    allocated = pd.Series(0.0, index=get_allocation_sectors())

    # Because the emission-to-be-allocated is defined as "Natural Gas to Chemical Plants",
    # here we only allocate emissions from non-energy use of natural gas to chemical industries (325XXX)
    logger.info("NOT reverting to V5 allocation changes.")
    mecs_2_1 = load_mecs_2_1()["Natural Gas(c)"]
    mecs_2_1_chemicals = mecs_2_1[mecs_2_1.index.str.startswith("325")]
    mecs_2_1_chemicals_sum = mecs_2_1["325"]

    for (
        ceda_industries,
        mecs_mappings,
    ) in mapping.items():
        inds = list(ceda_industries)
        total_use_ser = use.reindex(inds, fill_value=0.0)
        total_use: float = float(total_use_ser.sum())
        if total_use == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_chemicals_subtotal: float = mecs_2_1_chemicals[
            [m for m in mecs_mappings if m in mecs_2_1_chemicals.index]
        ].sum()
        for ceda_industry in ceda_industries:
            allocated[ceda_industry] = (
                emissions
                * (mecs_chemicals_subtotal / mecs_2_1_chemicals_sum)
                * float(total_use_ser[ceda_industry])
                / total_use
            )
    for ceda_industries, (
        mecs_mappings,
        subtract_mappings,
    ) in subtraction_mapping.items():
        inds_sub = list(ceda_industries)
        total_use_ser_sub = use.reindex(inds_sub, fill_value=0.0)
        total_use_sub: float = float(total_use_ser_sub.sum())
        if total_use_sub == 0:
            # If the total use is 0, we can't allocate anything
            # and we'll get a NaN so just leave as 0
            continue
        mecs_total: float = mecs_2_1_chemicals.loc[
            [m for m in mecs_mappings if m in mecs_2_1_chemicals.index]
        ].sum()
        subtract_mappings = [m for m in subtract_mappings if m in mecs_2_1_chemicals.index]  # type: ignore
        subtraction_total: float = mecs_2_1_chemicals.loc[
            list(subtract_mappings),
        ].sum()
        allocated_total = mecs_total - subtraction_total
        for ceda_industry in ceda_industries:
            industry_use = float(total_use_ser_sub[ceda_industry])
            allocated[ceda_industry] = (
                emissions
                * (allocated_total / mecs_2_1_chemicals_sum)
                * industry_use
                / total_use_sub
            )
    # There might be small under/over allocation due to independent rounding in MECS 2.1 table
    # Force the sum to be equal to emissions if 5% difference, otherwise raise an error
    if np.isclose(allocated.sum(), emissions, rtol=5e-2):
        allocated = emissions * allocated / allocated.sum()
    else:
        raise ValueError(
            f"Allocated emissions {allocated.sum()} MMT do not match total emissions {emissions} MMT."
        )

    return allocated * MEGATONNE_TO_KG
