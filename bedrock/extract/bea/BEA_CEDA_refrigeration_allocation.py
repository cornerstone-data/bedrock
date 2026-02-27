from __future__ import annotations

"""
Build an FBA from derive_make_use_ratios_for_hfcs_from_other_sources for use as
the attribution source in EPA_GHGI_T_4_124 (refrigerants_AC). Follows the same
pattern as BEA_CEDA_transport_allocation.py.
"""

from typing import Any, List

import pandas as pd

from bedrock.utils.mapping.location import US_FIPS


def refrigeration_make_use_ratios(
    *, df_list: Any, year: int | str, **_: Any
) -> pd.DataFrame:
    """
    Build an FBA from derive_make_use_ratios_for_hfcs_from_other_sources
    (allocation/other_gases/common_ratios). Used as proportional attribution
    source for refrigerants_AC in GHG_national_CEDA_common.

    FlowAmount: fraction of refrigeration HFC allocated to that sector (0–1).
    """
    from bedrock.transform.allocation.other_gases.common_ratios import (  # noqa: PLC0415
        derive_make_use_ratios_for_hfcs_from_other_sources,
    )

    weights = derive_make_use_ratios_for_hfcs_from_other_sources()
    rows: List[dict[str, Any]] = []
    for sector, frac in weights.items():
        rows.append(
            {
                "ActivityConsumedBy": sector,
                "ActivityProducedBy": "None",
                "FlowName": "HFC Share",
                "FlowAmount": float(frac),
                "Year": int(year),
                "Location": US_FIPS,
                "Unit": "fraction",
                "Class": "Other",
            }
        )
    df = pd.DataFrame(rows)
    return df
