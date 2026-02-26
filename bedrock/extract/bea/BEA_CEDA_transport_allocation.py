# BEA_CEDA_transport_allocation.py
"""
Build an FBA from derive_fuel_percent_breakout for use as the attribution source
for petroleum_fuels_gasoline and petroleum_fuels_diesel in GHG CEDA (3_13).
Replaces BEA_Detail_Use_AfterRedef for those activity sets.
"""
from __future__ import annotations

from typing import Any, List

import pandas as pd

from bedrock.utils.mapping.location import US_FIPS


def ceda_transport_allocation_parse(*, year: int | str, **_: Any) -> pd.DataFrame:
    """
    Build an FBA from derive_fuel_percent_breakout (sector × fuel_type shares).
    Used as proportional attribution source for 3_13 petroleum_fuels_gasoline
    and petroleum_fuels_diesel in GHG_national_CEDA_common.

    FlowName: fuel type string (e.g. "Motor Gasoline", "Distillate Fuel Oil").
    ActivityConsumedBy: BEA sector code.
    FlowAmount: fraction of that fuel allocated to that sector (0–1).
    """
    from bedrock.transform.allocation.transportation_fuel_use.derived import (  # noqa: PLC0415
        derive_fuel_percent_breakout,
    )

    breakout = derive_fuel_percent_breakout()
    rows: List[dict[str, Any]] = []
    for idx, frac in breakout.items():
        fuel_type, sector = idx[0], idx[1]  # type: ignore[index]
        flow_name = fuel_type.value if hasattr(fuel_type, "value") else str(fuel_type)
        rows.append(
            {
                "ActivityConsumedBy": sector,
                "FlowName": flow_name,
                "FlowAmount": float(frac),
                "Year": year,
                "Location": US_FIPS,
                "Unit": "fraction",
                "Class": "Other",
            }
        )
    return pd.DataFrame(rows)
