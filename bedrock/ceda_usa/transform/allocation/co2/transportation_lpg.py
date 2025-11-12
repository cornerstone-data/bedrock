from __future__ import annotations

import pandas as pd

from bedrock.ceda_usa.transform.allocation.co2.fuel_usage import (
    allocate_transportation_fuel_usage,
)
from bedrock.ceda_usa.transform.allocation.transportation_fuel_use.constants import (
    TRANSPORTATION_FUEL_TYPES,
)


def allocate_transportation_lpg() -> pd.Series[float]:
    return allocate_transportation_fuel_usage(TRANSPORTATION_FUEL_TYPES.LPG)
