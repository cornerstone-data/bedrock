from __future__ import annotations

import pandas as pd

from bedrock.transform.allocation.utils import get_allocation_sectors
from bedrock.utils.economic.units import MEGATONNE_TO_KG


def allocate_international_bunker_fuels() -> pd.Series[float]:
    """
    intentionally left blank
    """
    allocated = pd.Series(dtype=float)
    return allocated.reindex(get_allocation_sectors(), fill_value=0.0) * MEGATONNE_TO_KG
