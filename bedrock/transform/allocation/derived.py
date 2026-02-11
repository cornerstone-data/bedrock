from __future__ import annotations

import logging
import time

import pandas as pd

from bedrock.transform.allocation.constants import EmissionsSource
from bedrock.transform.allocation.registry import ALLOCATED_EMISSIONS_REGISTRY
from bedrock.utils.emissions.ghg import GHG_MAPPING
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix

logger = logging.getLogger(__name__)


def derive_E_usa() -> pd.DataFrame:
    # aggregate E from 15 gases to 7 gases
    return create_correspondence_matrix(GHG_MAPPING).T @ derive_E_usa_by_gas()


def derive_E_usa_by_gas() -> pd.DataFrame:
    return (
        derive_E_usa_emissions_sources()
        .groupby(lambda es: EmissionsSource(es).gas, axis=0)  # type: ignore
        .sum()
    )


def derive_E_usa_emissions_sources() -> pd.DataFrame:
    E_usa = pd.DataFrame(
        0.0,
        index=[es.value for es in EmissionsSource],
        # NOTE: CEDA_V7_SECTORS is used as industry index here, E_usa will be divided by `g` already having CEDA_V7_SECTORS
        columns=CEDA_V7_SECTORS,
    )

    total_start = time.time()
    for es, allocator in ALLOCATED_EMISSIONS_REGISTRY.items():
        logger.info(f"Allocating {es}")
        allocated = allocator()
        if allocated.isna().any():
            raise ValueError(f"NaNs found in {es} allocator")
        E_usa.loc[es.value, :] += allocated

    logger.info(
        f"[TIMING] All {len(ALLOCATED_EMISSIONS_REGISTRY)} allocations completed in {time.time() - total_start:.1f}s"
    )

    return E_usa
