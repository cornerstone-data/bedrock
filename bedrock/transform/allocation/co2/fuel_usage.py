from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.allocation.epa import (
    load_mmt_co2e_across_fuel_types as _load_table_a17,
)
from bedrock.transform.allocation.transportation_fuel_use.constants import (
    TRANSPORTATION_FUEL_TYPES,
)
from bedrock.transform.allocation.transportation_fuel_use.derived import (
    derive_fuel_percent_breakout as _derive_fuel_percent_breakout,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

derive_fuel_percent_breakout = functools.cache(_derive_fuel_percent_breakout)
load_table_a17 = functools.cache(_load_table_a17)


def allocate_transportation_fuel_usage(
    fuel: TRANSPORTATION_FUEL_TYPES,
) -> pd.Series[float]:
    total_fuel_for_transport = load_table_a17().loc[fuel.value, "Trans"]
    fuel_percent_breakout = derive_fuel_percent_breakout()

    allocated_ser = pd.Series(0.0, index=CEDA_V7_SECTORS)
    for sector, percent in fuel_percent_breakout.loc[fuel].items():  # type: ignore
        if sector not in allocated_ser.index:
            # This is the case for F01000, which we may want to bring back!
            continue
        allocated_ser[sector] = total_fuel_for_transport * percent

    return allocated_ser * MEGATONNE_TO_KG
