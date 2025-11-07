from __future__ import annotations

import pandas as pd

from ceda_usa.transform.allocation.co2.fuel_usage import (
    allocate_transportation_fuel_usage,
)
from ceda_usa.transform.allocation.transportation_fuel_use.constants import (
    TRANSPORTATION_FUEL_TYPES,
)


def allocate_transportation_jet_fuel() -> pd.Series[float]:
    return allocate_transportation_fuel_usage(TRANSPORTATION_FUEL_TYPES.JET_FUEL)
